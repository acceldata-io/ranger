/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.patch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Updates the xstore service-def in place on existing deployments:
 *
 * <ul>
 *   <li>Heals invalid config-def types (e.g. {@code "number"} &rarr; {@code "int"}) that
 *       slipped past {@link org.apache.ranger.biz.ServiceDBStore#createServiceDef}'s
 *       lenient create-time validation. Without this heal,
 *       {@link RangerServiceDefValidator#validate} rejects any subsequent UPDATE.
 *   <li>Adds top-level resources that are present in the embedded service-def but missing
 *       from the DB (e.g. {@code column} for column-level masking). Required because every
 *       resource referenced from {@code dataMaskDef.resources} / {@code rowFilterDef.resources}
 *       must exist as a top-level resource for {@code ServiceDBStore.updateChildObjectsOfServiceDef}
 *       to persist its per-resource options.
 *   <li>Backfills {@code dataMaskDef} and {@code rowFilterDef} from the embedded
 *       service-def so the masking and row-filter forms appear in Ranger Admin.
 * </ul>
 *
 * <p>New deployments get the correct definitions via {@link EmbeddedServiceDefsUtil}'s
 * create-if-missing logic. Existing deployments need this patch because that utility
 * never overwrites an already-present service-def row, so any pre-existing corruption
 * or missing sub-defs persist forever unless explicitly updated.
 */
@Component
public class PatchForXstoreServiceDefUpdate_J10064 extends BaseLoader {
    private static final Logger LOG =
            LoggerFactory.getLogger(PatchForXstoreServiceDefUpdate_J10064.class);

    private static final String SERVICEDEF_NAME_XSTORE = "xstore";

    /**
     * Latches to {@code true} when {@link #execLoad()} fails. Required because
     * {@link BaseLoader#load()} has a catch-all that swallows every {@link Throwable},
     * so a re-throw from {@code execLoad} never reaches {@link #main(String[])}.
     * Without this flag, a failed patch run still exits 0 and the runner marks
     * the {@code J10064} row {@code active='Y'} &mdash; leaving partial state in
     * the DB and no easy retry path. The flag is {@code static} so the CGLIB
     * proxy and target instance share a single value.
     */
    private static volatile boolean failureOccurred = false;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RangerValidatorFactory validatorFactory;

    public static void main(String[] args) {
        LOG.info("main()");
        try {
            PatchForXstoreServiceDefUpdate_J10064 loader =
                    (PatchForXstoreServiceDefUpdate_J10064)
                            CLIUtil.getBean(PatchForXstoreServiceDefUpdate_J10064.class);
            loader.init();
            while (loader.isMoreToProcess()) {
                loader.load();
            }
            if (failureOccurred) {
                LOG.error(
                        "Patch did not complete successfully; see prior error logs. Exiting non-zero "
                                + "so db_setup.py will delete the J10064 tracker row and the patch will "
                                + "re-run on the next deploy.");
                System.exit(1);
            }
            LOG.info("Load complete. Exiting!");
            System.exit(0);
        } catch (Exception e) {
            LOG.error("Error loading", e);
            System.exit(1);
        }
    }

    @Override
    public void init() throws Exception {
        // No-op
    }

    @Override
    public void execLoad() {
        LOG.info("==> PatchForXstoreServiceDefUpdate_J10064.execLoad()");
        try {
            updateXstoreServiceDef();
        } catch (Exception e) {
            LOG.error("Error while updating xstore service-def", e);
            failureOccurred = true;
            // Re-throw so the @Transactional load() rolls back any uncommitted writes from
            // this attempt. BaseLoader.load() will still swallow this for moreToProcess
            // bookkeeping, which is why main() also checks the failureOccurred flag above.
            throw new RuntimeException(e);
        }
        LOG.info("<== PatchForXstoreServiceDefUpdate_J10064.execLoad()");
    }

    @Override
    public void printStats() {
        LOG.info("PatchForXstoreServiceDefUpdate_J10064 completed");
    }

    private void updateXstoreServiceDef() throws Exception {
        RangerServiceDef embeddedDef =
                EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(SERVICEDEF_NAME_XSTORE);
        if (embeddedDef == null) {
            LOG.warn("Embedded xstore service-def not found on classpath; skipping patch");
            return;
        }

        RangerServiceDef dbDef = svcStore.getServiceDefByName(SERVICEDEF_NAME_XSTORE);
        if (dbDef == null) {
            // Fresh deployment: EmbeddedServiceDefsUtil already inserted the latest def.
            LOG.info("xstore service-def not present in DB yet; nothing to patch");
            return;
        }

        boolean changed = false;

        // Heal historical bad config-def types so RangerServiceDefValidator passes on
        // Action.UPDATE. Ranger's strict types are: bool, enum, int, string, password,
        // path. Older xstore service-defs shipped with type="number" (a JSON-Schema'ism)
        // for the *.ms timeout configs; remap them to "int". Without this, the validator
        // call below rejects the update and the masking/row-filter backfill is lost.
        if (dbDef.getConfigs() != null) {
            for (RangerServiceConfigDef cfg : dbDef.getConfigs()) {
                if ("number".equals(cfg.getType())) {
                    LOG.info("Healing invalid config type 'number' -> 'int' for {}", cfg.getName());
                    cfg.setType("int");
                    changed = true;
                }
            }
        }

        // Add any top-level resources present in the embedded def but missing from the DB
        // (typically 'column' on upgrades that predate it). Required because every entry in
        // dataMaskDef.resources / rowFilterDef.resources is matched by name against existing
        // top-level resources at persistence time; a name with no matching top-level row
        // causes ServiceDBStore.updateChildObjectsOfServiceDef to throw, which silently
        // half-commits (mask types persist; per-resource options don't).
        if (embeddedDef.getResources() != null) {
            Set<String> dbResourceNames = new HashSet<>();
            if (dbDef.getResources() != null) {
                for (RangerResourceDef r : dbDef.getResources()) {
                    dbResourceNames.add(r.getName());
                }
            }
            for (RangerResourceDef embeddedRes : embeddedDef.getResources()) {
                if (!dbResourceNames.contains(embeddedRes.getName())) {
                    if (dbDef.getResources() == null) {
                        dbDef.setResources(new ArrayList<>());
                    }
                    dbDef.getResources().add(embeddedRes);
                    changed = true;
                    LOG.info(
                            "Added missing top-level resource '{}' from embedded service-def",
                            embeddedRes.getName());
                }
            }
        }

        if (isIncomplete(dbDef.getDataMaskDef())) {
            dbDef.setDataMaskDef(embeddedDef.getDataMaskDef());
            changed = true;
            LOG.info("Backfilled xstore.dataMaskDef from embedded service-def");
        }

        if (isIncomplete(dbDef.getRowFilterDef())) {
            dbDef.setRowFilterDef(embeddedDef.getRowFilterDef());
            changed = true;
            LOG.info("Backfilled xstore.rowFilterDef from embedded service-def");
        }

        if (!changed) {
            LOG.info("xstore service-def already current (configs healed, masking + row-filter populated); nothing to do");
            return;
        }

        RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
        validator.validate(dbDef, Action.UPDATE);
        svcStore.updateServiceDef(dbDef);
        LOG.info("Updated xstore service-def successfully");
    }

    /**
     * Returns {@code true} if {@code def} is missing any sub-list a usable masking form needs
     * (resources, maskTypes, or accessTypes). Stricter than a pure null/empty check so the
     * patch self-heals after a half-committed prior run &mdash; e.g. one where mask types
     * persisted but per-resource options didn't because of an earlier validation failure.
     */
    private static boolean isIncomplete(RangerServiceDef.RangerDataMaskDef def) {
        return def == null
                || def.getResources() == null || def.getResources().isEmpty()
                || def.getMaskTypes() == null || def.getMaskTypes().isEmpty()
                || def.getAccessTypes() == null || def.getAccessTypes().isEmpty();
    }

    private static boolean isIncomplete(RangerServiceDef.RangerRowFilterDef def) {
        return def == null
                || def.getResources() == null || def.getResources().isEmpty()
                || def.getAccessTypes() == null || def.getAccessTypes().isEmpty();
    }
}
