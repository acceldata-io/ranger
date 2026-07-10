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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

@Component
public class PatchForHiveServiceDefUpdate_J10064 extends BaseLoader {
	private static final Logger logger = LoggerFactory.getLogger(PatchForHiveServiceDefUpdate_J10064.class);

	public static final String SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME = "hive";
	public static final String STORAGE_TYPE_RESOURCE_NAME                = "storage-type";
	public static final String STORAGE_URL_RESOURCE_NAME                 = "storage-url";
	public static final String RWSTORAGE_ACCESS_TYPE_NAME                = "rwstorage";
	public static final String ALL_ACCESS_TYPE_NAME                      = "all";

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	ServiceDBStore svcDBStore;

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	RangerPolicyService policyService;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	XPolicyService xPolService;

	@Autowired
	XPermMapService xPermMapService;

	@Autowired
	RangerBizUtil bizUtil;

	@Autowired
	RangerValidatorFactory validatorFactory;

	@Autowired
	ServiceDBStore svcStore;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchForHiveServiceDefUpdate_J10064 loader = (PatchForHiveServiceDefUpdate_J10064) CLIUtil.getBean(PatchForHiveServiceDefUpdate_J10064.class);
			loader.init();
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			logger.info("Load complete. Exiting!!!");
			System.exit(0);
		} catch (Exception e) {
			logger.error("Error loading", e);
			System.exit(1);
		}
	}

	@Override
	public void init() throws Exception {
		// Do Nothing
	}

	@Override
	public void execLoad() {
		logger.info("==> PatchForHiveServiceDefUpdate_J10064.execLoad()");
		try {
			updateHiveServiceDef();
		} catch (Exception e) {
			logger.error("Error while updating hive service-def with storage-type/storage-url resources and rwstorage access type", e);
		}
		logger.info("<== PatchForHiveServiceDefUpdate_J10064.execLoad()");
	}

	@Override
	public void printStats() {
		logger.info("PatchForHiveServiceDefUpdate_J10064 data ");
	}

	private void updateHiveServiceDef() throws Exception {
		RangerServiceDef embeddedHiveServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME);

		if (embeddedHiveServiceDef == null) {
			logger.error("The embedded Hive service-definition does not exist.");
			return;
		}

		XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME);
		if (xXServiceDefObj == null) {
			logger.info("Hive service-definition is not present in the DB. Fresh install path — nothing to patch.");
			return;
		}

		RangerServiceDef dbHiveServiceDef = svcDBStore.getServiceDefByName(SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME);
		if (dbHiveServiceDef == null) {
			logger.error("Hive service-definition does not exist in the db store.");
			return;
		}

		boolean isServiceDefUpdated = mergeStorageResourcesAndAccessType(dbHiveServiceDef, embeddedHiveServiceDef);
		if (!isServiceDefUpdated) {
			logger.info("Hive service-def already contains storage-type/storage-url resources and rwstorage access type. Nothing to do.");
			return;
		}

		RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
		validator.validate(dbHiveServiceDef, Action.UPDATE);
		svcStore.updateServiceDef(dbHiveServiceDef);
		logger.info("Hive service-def updated with storage-type/storage-url resources and rwstorage access type.");
	}

	private boolean mergeStorageResourcesAndAccessType(RangerServiceDef dbServiceDef, RangerServiceDef embeddedServiceDef) {
		boolean updated = false;

		List<RangerServiceDef.RangerResourceDef>   dbResources         = dbServiceDef.getResources();
		List<RangerServiceDef.RangerAccessTypeDef> dbAccessTypes       = dbServiceDef.getAccessTypes();
		List<RangerServiceDef.RangerResourceDef>   embeddedResources   = embeddedServiceDef.getResources();
		List<RangerServiceDef.RangerAccessTypeDef> embeddedAccessTypes = embeddedServiceDef.getAccessTypes();

		for (String resourceName : new String[] { STORAGE_TYPE_RESOURCE_NAME, STORAGE_URL_RESOURCE_NAME }) {
			if (findResource(dbResources, resourceName) != null) {
				continue;
			}
			RangerServiceDef.RangerResourceDef embeddedResource = findResource(embeddedResources, resourceName);
			if (embeddedResource == null) {
				logger.warn("Embedded Hive service-def is missing resource '{}'; skipping.", resourceName);
				continue;
			}
			dbResources.add(embeddedResource);
			updated = true;
		}

		if (findAccessType(dbAccessTypes, RWSTORAGE_ACCESS_TYPE_NAME) == null) {
			RangerServiceDef.RangerAccessTypeDef embeddedRwStorage = findAccessType(embeddedAccessTypes, RWSTORAGE_ACCESS_TYPE_NAME);
			if (embeddedRwStorage != null) {
				dbAccessTypes.add(embeddedRwStorage);
				updated = true;
			} else {
				logger.warn("Embedded Hive service-def is missing access type '{}'; skipping.", RWSTORAGE_ACCESS_TYPE_NAME);
			}
		}

		RangerServiceDef.RangerAccessTypeDef dbAll = findAccessType(dbAccessTypes, ALL_ACCESS_TYPE_NAME);
		if (dbAll != null) {
			Collection<String> impliedGrants = dbAll.getImpliedGrants();
			if (impliedGrants != null && !impliedGrants.contains(RWSTORAGE_ACCESS_TYPE_NAME)) {
				impliedGrants.add(RWSTORAGE_ACCESS_TYPE_NAME);
				updated = true;
			}
		}

		return updated;
	}

	private RangerServiceDef.RangerResourceDef findResource(List<RangerServiceDef.RangerResourceDef> resources, String name) {
		if (resources == null) {
			return null;
		}
		for (RangerServiceDef.RangerResourceDef r : resources) {
			if (StringUtils.equals(r.getName(), name)) {
				return r;
			}
		}
		return null;
	}

	private RangerServiceDef.RangerAccessTypeDef findAccessType(List<RangerServiceDef.RangerAccessTypeDef> accessTypes, String name) {
		if (accessTypes == null) {
			return null;
		}
		for (RangerServiceDef.RangerAccessTypeDef a : accessTypes) {
			if (StringUtils.equals(a.getName(), name)) {
				return a;
			}
		}
		return null;
	}
}
