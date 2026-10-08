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

package org.apache.ranger.unixusersync.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.ugsyncutil.model.XGroupInfo;
import org.apache.ranger.ugsyncutil.model.XUserInfo;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestPolicyMgrUserGroupBuilderVisibility {

	private static final String IS_VISIBLE = "1";
	private static final String IS_HIDDEN = "0";
	private static final String SYNC_SOURCE = "Admin Central";

	private PolicyMgrUserGroupBuilder sink;

	@BeforeEach
	void setUp() {
		sink = new PolicyMgrUserGroupBuilder();
	}

	@Test
	void computeUserDeltaRestoresVisibilityWhenSoftDeletedUserReappears() {
		String userName = "alice";
		Map<String, String> attrs = attrsFor(userName);

		XUserInfo cached = new XUserInfo();
		cached.setName(userName);
		cached.setSyncSource(SYNC_SOURCE);
		cached.setOtherAttrsMap(attrs);
		cached.setOtherAttributes(JsonUtils.objectToJson(attrs));
		cached.setIsVisible(IS_HIDDEN);

		Map<String, XUserInfo> userCache = new HashMap<>();
		userCache.put(userName, cached);
		Map<String, String> userNameMap = new HashMap<>();
		userNameMap.put(userName, userName);
		sink.setCachesForTest(userCache, null, userNameMap, null);

		Map<String, Map<String, String>> sourceUsers = new HashMap<>();
		sourceUsers.put(userName, attrs);

		Map<String, XUserInfo> delta = sink.computeUserDeltaForTest(sourceUsers);

		assertTrue(delta.containsKey(userName));
		assertEquals(IS_VISIBLE, delta.get(userName).getIsVisible());
		assertEquals(IS_VISIBLE, cached.getIsVisible());
	}

	@Test
	void computeUserDeltaSkipsAlreadyVisibleUserWithNoChanges() {
		String userName = "bob";
		Map<String, String> attrs = attrsFor(userName);

		XUserInfo cached = new XUserInfo();
		cached.setName(userName);
		cached.setSyncSource(SYNC_SOURCE);
		cached.setOtherAttrsMap(attrs);
		cached.setOtherAttributes(JsonUtils.objectToJson(attrs));
		cached.setIsVisible(IS_VISIBLE);

		Map<String, XUserInfo> userCache = new HashMap<>();
		userCache.put(userName, cached);
		Map<String, String> userNameMap = new HashMap<>();
		userNameMap.put(userName, userName);
		sink.setCachesForTest(userCache, null, userNameMap, null);

		Map<String, Map<String, String>> sourceUsers = new HashMap<>();
		sourceUsers.put(userName, attrs);

		Map<String, XUserInfo> delta = sink.computeUserDeltaForTest(sourceUsers);

		assertFalse(delta.containsKey(userName));
		assertEquals(IS_VISIBLE, cached.getIsVisible());
	}

	@Test
	void computeGroupDeltaRestoresVisibilityWhenSoftDeletedGroupReappears() {
		String groupName = "engineers";
		Map<String, String> attrs = attrsFor(groupName);

		XGroupInfo cached = new XGroupInfo();
		cached.setName(groupName);
		cached.setSyncSource(SYNC_SOURCE);
		cached.setOtherAttrsMap(attrs);
		cached.setOtherAttributes(JsonUtils.objectToJson(attrs));
		cached.setIsVisible(IS_HIDDEN);

		Map<String, XGroupInfo> groupCache = new HashMap<>();
		groupCache.put(groupName, cached);
		Map<String, String> groupNameMap = new HashMap<>();
		groupNameMap.put(groupName, groupName);
		sink.setCachesForTest(null, groupCache, null, groupNameMap);

		Map<String, Map<String, String>> sourceGroups = new HashMap<>();
		sourceGroups.put(groupName, attrs);

		Map<String, XGroupInfo> delta = sink.computeGroupDeltaForTest(sourceGroups);

		assertTrue(delta.containsKey(groupName));
		assertEquals(IS_VISIBLE, delta.get(groupName).getIsVisible());
		assertEquals(IS_VISIBLE, cached.getIsVisible());
	}

	private static Map<String, String> attrsFor(String name) {
		Map<String, String> attrs = new HashMap<>();
		attrs.put(UgsyncCommonConstants.ORIGINAL_NAME, name);
		attrs.put(UgsyncCommonConstants.FULL_NAME, name);
		attrs.put(UgsyncCommonConstants.SYNC_SOURCE, SYNC_SOURCE);
		return attrs;
	}
}
