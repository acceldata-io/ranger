/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.usergroupsync;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserGroupSync implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(UserGroupSync.class);

	private UserGroupSink ugSink;
	private UserGroupSource ugSource;

	/**
	 * Second-truncated baseline. {@code File.lastModified()} is 1-second resolution on most
	 * filesystems, so a millisecond timestamp in the same second looks newer than a fresh touch.
	 */
	private long lastForceSyncTriggerProcessedAt = floorToSecond(System.currentTimeMillis());

	/** mtime already acted on. A second touch in that same second is not a new event. */
	private long lastForceSyncTriggerMtime = Long.MIN_VALUE;

	public static void main(String[] args) {
		UserGroupSync userGroupSync = new UserGroupSync();
		userGroupSync.run();
	}

	@Override
	public void run() {
		try {
			long sleepTimeBetweenCycleInMillis = UserGroupSyncConfig.getInstance().getSleepTimeInMillisBetweenCycle();
			long initSleepTimeBetweenCycleInMillis = UserGroupSyncConfig.getInstance().getInitSleepTimeInMillisBetweenCycle();
			boolean initPending = true;

			while (initPending) {
				try {
					if (UserGroupSyncConfig.isUgsyncServiceActive()) {
						ugSink = UserGroupSyncConfig.getInstance().getUserGroupSink();
						LOG.info("initializing sink: " + ugSink.getClass().getName());
						ugSink.init();

						ugSource = UserGroupSyncConfig.getInstance().getUserGroupSource();
						LOG.info("initializing source: " + ugSource.getClass().getName());
						ugSource.init();

						LOG.info("Begin: initial load of user/group from source==>sink");
						syncUserGroup();
						LOG.info("End: initial load of user/group from source==>sink");

						lastForceSyncTriggerProcessedAt = floorToSecond(System.currentTimeMillis());
						lastForceSyncTriggerMtime = Long.MIN_VALUE;
						initPending = false;
						LOG.info("Done initializing user/group source and sink");
					}else {
						if (LOG.isDebugEnabled()){
							LOG.debug("Sleeping for [" + initSleepTimeBetweenCycleInMillis + "] milliSeconds as this server is running in passive mode");
						}
						Thread.sleep(initSleepTimeBetweenCycleInMillis);
					}
				} catch (Throwable t) {
					LOG.error("Failed to initialize UserGroup source/sink. Will retry after " + sleepTimeBetweenCycleInMillis + " milliseconds. Error details: ", t);
					try {
						if (LOG.isDebugEnabled()){
							LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");
						}
						Thread.sleep(sleepTimeBetweenCycleInMillis);
					} catch (Exception e) {
						LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to initialize UserGroup source/sink", e);
					}
				}
			}

			while (true) {
				try {
					if (LOG.isDebugEnabled()){
						LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");
					}
					sleepBetweenSyncCycles(sleepTimeBetweenCycleInMillis);
				} catch (InterruptedException e) {
					LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to synchronize UserGroup information", e);
				}

				try {
					if (UserGroupSyncConfig.isUgsyncServiceActive()) {
						LOG.info("Begin: update user/group from source==>sink");
						syncUserGroup();
						LOG.info("End: update user/group from source==>sink");
					} else {
						LOG.info("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds as this server is running in passive mode");
					}
				} catch (Throwable t) {
					LOG.error("Failed to synchronize UserGroup information. Error details: ", t);
				}
			}

		} catch (Throwable t) {
			LOG.error("UserGroupSync thread got an error", t);
		} finally {
			LOG.info("Shutting down the UserGroupSync thread");
		}
	}

	private static long floorToSecond(long epochMillis) {
		return epochMillis / 1000L * 1000L;
	}

	private void syncUserGroup() throws Throwable {
		UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();

		if (config.isUserSyncEnabled()) {
			ugSource.updateSink(ugSink);
		}

	}

	private void sleepBetweenSyncCycles(long sleepTimeBetweenCycleInMillis) throws InterruptedException {
		UserGroupSyncConfig cfg = UserGroupSyncConfig.getInstance();
		String triggerFile = cfg.getForceSyncTriggerFile();
		if (StringUtils.isBlank(triggerFile) || sleepTimeBetweenCycleInMillis <= 0) {
			Thread.sleep(sleepTimeBetweenCycleInMillis);
			return;
		}
		long remaining = sleepTimeBetweenCycleInMillis;
		long chunk = Math.min(10_000L, Math.max(1_000L, sleepTimeBetweenCycleInMillis));
		File f = new File(triggerFile);
		while (remaining > 0) {
			long thisSleep = Math.min(chunk, remaining);
			Thread.sleep(thisSleep);
			remaining -= thisSleep;
			long modifiedAt = f.lastModified();
			if (f.exists() && modifiedAt >= lastForceSyncTriggerProcessedAt && modifiedAt != lastForceSyncTriggerMtime) {
				lastForceSyncTriggerMtime = modifiedAt;
				lastForceSyncTriggerProcessedAt = modifiedAt;
				LOG.info("Force sync trigger file was updated; resuming user/group sync early.");
				return;
			}
		}
	}

}
