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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.ugsyncutil.model.FileSyncSourceInfo;
import org.apache.ranger.ugsyncutil.model.UgsyncAuditInfo;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.usergroupsync.AbstractUserGroupSource;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usergroupsync.UserGroupSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * UserGroupSource that reads users and groups from the XDP Admin Central HTTP API
 * (Keycloak as the backing identity store).
 */
public class AdminCentralUserGroupBuilder extends AbstractUserGroupSource implements UserGroupSource {

	private static final Logger LOG = LoggerFactory.getLogger(AdminCentralUserGroupBuilder.class);

	private final ObjectMapper mapper = new ObjectMapper();

	private Map<String, Map<String, String>> sourceUsers;
	private Map<String, Map<String, String>> sourceGroups;
	private Map<String, Set<String>> sourceGroupUsers;

	private UgsyncAuditInfo ugsyncAuditInfo;
	private FileSyncSourceInfo fileSyncSourceInfo;
	private int deleteCycles;
	private boolean computeDeletes = false;
	private String currentSyncSource;
	private boolean isUpdateSinkSucc = true;

	private AdminCentralRestClient restClient;
	private boolean userNameCaseConversionFlag;
	private boolean userNameLowerCaseFlag;
	private boolean groupNameCaseConversionFlag;
	private boolean groupNameLowerCaseFlag;

	public AdminCentralUserGroupBuilder() {
		super();
		String userNameCaseConversion = config.getUserNameCaseConversion();
		if (UserGroupSyncConfig.UGSYNC_NONE_CASE_CONVERSION_VALUE.equalsIgnoreCase(userNameCaseConversion)) {
			userNameCaseConversionFlag = false;
		} else {
			userNameCaseConversionFlag = true;
			userNameLowerCaseFlag =
					UserGroupSyncConfig.UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(userNameCaseConversion);
		}
		String groupNameCaseConversion = config.getGroupNameCaseConversion();
		if (UserGroupSyncConfig.UGSYNC_NONE_CASE_CONVERSION_VALUE.equalsIgnoreCase(groupNameCaseConversion)) {
			groupNameCaseConversionFlag = false;
		} else {
			groupNameCaseConversionFlag = true;
			groupNameLowerCaseFlag =
					UserGroupSyncConfig.UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(groupNameCaseConversion);
		}
	}

	@Override
	public void init() throws Throwable {
		deleteCycles = 1;
		currentSyncSource = config.getCurrentSyncSource();
		ugsyncAuditInfo = new UgsyncAuditInfo();
		fileSyncSourceInfo = new FileSyncSourceInfo();
		ugsyncAuditInfo.setSyncSource(currentSyncSource);
		ugsyncAuditInfo.setFileSyncSourceInfo(fileSyncSourceInfo);
		fileSyncSourceInfo.setFileName(config.getAdminCentralBaseUrl() + config.getAdminCentralUsersPath());

		restClient =
				new AdminCentralRestClient(
						config.getAdminCentralConnectTimeoutMs(),
						config.getAdminCentralReadTimeoutMs(),
						config.getAdminCentralAuthType(),
						config.getAdminCentralUsername(),
						config.getAdminCentralPassword(),
						config.getAdminCentralBearerToken());

		buildUserGroupInfo();
	}

	@Override
	public boolean isChanged() {
		// Remote HTTP source: do not diff payloads; poll every cycle (same idea as LdapUserGroupBuilder).
		return true;
	}

	@Override
	public void updateSink(UserGroupSink sink) throws Throwable {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date syncTime = new Date(System.currentTimeMillis());
		fileSyncSourceInfo.setLastModified(formatter.format(syncTime));
		fileSyncSourceInfo.setSyncTime(formatter.format(syncTime));

		computeDeletes = false;
		if (!isUpdateSinkSucc) {
			LOG.info("Previous updateSink failed and hence retry!!");
			isUpdateSinkSucc = true;
		}
		try {
			if (config.isUserSyncDeletesEnabled() && deleteCycles >= config.getUserSyncDeletesFrequency()) {
				deleteCycles = 1;
				computeDeletes = true;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Compute deleted users/groups is enabled for this sync cycle");
				}
			}
		} catch (Throwable t) {
			LOG.error("Failed to get information about usersync delete frequency", t);
		}
		if (config.isUserSyncDeletesEnabled()) {
			deleteCycles++;
		}

		buildUserGroupInfo();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Users = {}", sourceUsers.keySet());
			LOG.debug("Groups = {}", sourceGroups.keySet());
			LOG.debug("GroupUsers = {}", sourceGroupUsers.keySet());
		}
		try {
			sink.addOrUpdateUsersGroups(sourceGroups, sourceUsers, sourceGroupUsers, computeDeletes);
		} catch (Throwable t) {
			LOG.error("Failed to update ranger admin. Will retry in next sync cycle!!", t);
			isUpdateSinkSucc = false;
		}
		try {
			sink.postUserGroupAuditInfo(ugsyncAuditInfo);
		} catch (Throwable t) {
			LOG.error("sink.postUserGroupAuditInfo failed with exception: {}", t.getMessage());
		}
	}

	private void buildUserGroupInfo() throws Throwable {
		sourceUsers = new HashMap<>();
		sourceGroups = new HashMap<>();
		sourceGroupUsers = new HashMap<>();

		String usersArrayPath = config.getAdminCentralUsersArrayPath();
		String userNameField = config.getAdminCentralUserNameField();
		String userGroupsField = config.getAdminCentralUserGroupsField();
		String enabledField = config.getAdminCentralUserEnabledField();

		fetchAndMergeUsers(usersArrayPath, userNameField, userGroupsField, enabledField);

		String groupsPath = config.getAdminCentralGroupsPath();
		if (StringUtils.isNotBlank(groupsPath)) {
			String groupsArrayPath = config.getAdminCentralGroupsArrayPath();
			String groupNameField = config.getAdminCentralGroupNameField();
			String membersField = config.getAdminCentralGroupMembersField();
			fetchAndMergeGroups(groupsPath, groupsArrayPath, groupNameField, membersField);
		}
	}

	private void fetchAndMergeUsers(
			String usersArrayPath, String userNameField, String userGroupsField, String enabledField)
			throws Throwable {
		String base = config.getAdminCentralBaseUrl() + config.getAdminCentralUsersPath();
		int pageSize = config.getAdminCentralPageSize();
		String sizeParam = config.getAdminCentralPageSizeParam();
		String pageParam = config.getAdminCentralPageIndexParam();

		int page = 0;
		while (true) {
			String url = base;
			if (pageSize > 0) {
				String sep = base.contains("?") ? "&" : "?";
				url = base + sep + pageParam + "=" + page + "&" + sizeParam + "=" + pageSize;
			}
			String json = restClient.getJson(url);
			JsonNode root = mapper.readTree(json);
			JsonNode arrNode = AdminCentralResponseParser.navigate(root, usersArrayPath);
			ArrayNode arr = AdminCentralResponseParser.asArray(arrNode);
			if (arr == null || arr.size() == 0) {
				if (page == 0 && arr == null) {
					LOG.warn(
							"No user array found at JSON path \"{}\"; check ranger.usersync.admincentral.users.array.path",
							usersArrayPath);
				}
				break;
			}
			for (JsonNode u : arr) {
				mergeUser(u, userNameField, userGroupsField, enabledField);
			}
			if (pageSize <= 0) {
				break;
			}
			if (arr.size() < pageSize) {
				break;
			}
			page++;
		}
	}

	private void mergeUser(JsonNode user, String userNameField, String userGroupsField, String enabledField) {
		if (!AdminCentralResponseParser.isEffectivelyEnabled(user, enabledField)) {
			return;
		}
		JsonNode nameNode = user.get(userNameField);
		String rawName = AdminCentralResponseParser.textOrNull(nameNode);
		if (StringUtils.isBlank(rawName)) {
			return;
		}
		if (userNameRegExInst != null) {
			rawName = userNameRegExInst.transform(rawName);
		}
		String userName = applyUserCase(rawName);
		if (StringUtils.isBlank(userName)) {
			return;
		}

		Map<String, String> userAttrMap = new HashMap<>();
		userAttrMap.put(UgsyncCommonConstants.ORIGINAL_NAME, userName);
		userAttrMap.put(UgsyncCommonConstants.FULL_NAME, userName);
		userAttrMap.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
		sourceUsers.put(userName, userAttrMap);

		JsonNode groupsNode = user.get(userGroupsField);
		if (groupsNode != null && groupsNode.isArray()) {
			for (JsonNode g : groupsNode) {
				String groupName = groupNameFromNode(g, config.getAdminCentralGroupNameField());
				if (StringUtils.isNotBlank(groupName)) {
					linkUserToGroup(userName, groupName);
				}
			}
		}
	}

	private static String groupNameFromNode(JsonNode g, String objectNameField) {
		if (g == null || g.isNull()) {
			return null;
		}
		if (g.isTextual()) {
			return g.asText();
		}
		if (g.isObject()) {
			return AdminCentralResponseParser.textOrNull(g.get(objectNameField));
		}
		return g.asText();
	}

	private void linkUserToGroup(String userName, String rawGroupName) {
		String gn = rawGroupName;
		if (groupNameRegExInst != null) {
			gn = groupNameRegExInst.transform(gn);
		}
		gn = applyGroupCase(gn);
		if (StringUtils.isBlank(gn)) {
			return;
		}
		Map<String, String> groupAttrMap = new HashMap<>();
		groupAttrMap.put(UgsyncCommonConstants.ORIGINAL_NAME, gn);
		groupAttrMap.put(UgsyncCommonConstants.FULL_NAME, gn);
		groupAttrMap.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
		sourceGroups.put(gn, groupAttrMap);

		addUserToGroupMembership(gn, userName);
	}

	/**
	 * Records user membership for a group whose name is already normalized and present in {@link
	 * #sourceGroups} (e.g. from {@link #mergeGroup}). Skips re-applying group mapping/case rules.
	 */
	private void linkUserToNormalizedGroup(String userName, String normalizedGroupName) {
		if (StringUtils.isBlank(normalizedGroupName)) {
			return;
		}
		addUserToGroupMembership(normalizedGroupName, userName);
	}

	private void addUserToGroupMembership(String normalizedGroupName, String userName) {
		Set<String> members = sourceGroupUsers.get(normalizedGroupName);
		if (members == null) {
			members = new HashSet<>();
		}
		members.add(userName);
		sourceGroupUsers.put(normalizedGroupName, members);
	}

	private void fetchAndMergeGroups(
			String groupsPath,
			String groupsArrayPath,
			String groupNameField,
			String membersField)
			throws Throwable {
		String base = config.getAdminCentralBaseUrl() + groupsPath;
		int pageSize = config.getAdminCentralPageSize();
		String sizeParam = config.getAdminCentralPageSizeParam();
		String pageParam = config.getAdminCentralPageIndexParam();

		int page = 0;
		while (true) {
			String url = base;
			if (pageSize > 0) {
				String sep = base.contains("?") ? "&" : "?";
				url = base + sep + pageParam + "=" + page + "&" + sizeParam + "=" + pageSize;
			}
			String json = restClient.getJson(url);
			JsonNode root = mapper.readTree(json);
			JsonNode arrNode = AdminCentralResponseParser.navigate(root, groupsArrayPath);
			ArrayNode arr = AdminCentralResponseParser.asArray(arrNode);
			if (arr == null || arr.size() == 0) {
				break;
			}
			for (JsonNode g : arr) {
				mergeGroup(g, groupNameField, membersField);
			}
			if (pageSize <= 0) {
				break;
			}
			if (arr.size() < pageSize) {
				break;
			}
			page++;
		}
	}

	private void mergeGroup(JsonNode group, String groupNameField, String membersField) {
		String rawName = AdminCentralResponseParser.textOrNull(group.get(groupNameField));
		if (StringUtils.isBlank(rawName)) {
			return;
		}
		if (groupNameRegExInst != null) {
			rawName = groupNameRegExInst.transform(rawName);
		}
		String groupName = applyGroupCase(rawName);
		if (StringUtils.isBlank(groupName)) {
			return;
		}
		Map<String, String> groupAttrMap = new HashMap<>();
		groupAttrMap.put(UgsyncCommonConstants.ORIGINAL_NAME, groupName);
		groupAttrMap.put(UgsyncCommonConstants.FULL_NAME, groupName);
		groupAttrMap.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
		sourceGroups.put(groupName, groupAttrMap);

		JsonNode mem = group.get(membersField);
		if (mem != null && mem.isArray()) {
			for (JsonNode m : mem) {
				String memberName = memberUserName(m, config.getAdminCentralUserNameField());
				if (StringUtils.isNotBlank(memberName)) {
					if (userNameRegExInst != null) {
						memberName = userNameRegExInst.transform(memberName);
					}
					memberName = applyUserCase(memberName);
					Map<String, String> uAttr = new HashMap<>();
					uAttr.put(UgsyncCommonConstants.ORIGINAL_NAME, memberName);
					uAttr.put(UgsyncCommonConstants.FULL_NAME, memberName);
					uAttr.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
					sourceUsers.put(memberName, uAttr);
					linkUserToNormalizedGroup(memberName, groupName);
				}
			}
		}
	}

	private String memberUserName(JsonNode m, String userNameField) {
		if (m == null || m.isNull()) {
			return null;
		}
		if (m.isTextual()) {
			return m.asText();
		}
		if (m.isObject()) {
			return AdminCentralResponseParser.textOrNull(m.get(userNameField));
		}
		return m.asText();
	}

	private String applyUserCase(String name) {
		if (!userNameCaseConversionFlag || name == null) {
			return name;
		}
		return userNameLowerCaseFlag ? name.toLowerCase() : name.toUpperCase();
	}

	private String applyGroupCase(String name) {
		if (!groupNameCaseConversionFlag || name == null) {
			return name;
		}
		return groupNameLowerCaseFlag ? name.toLowerCase() : name.toUpperCase();
	}
}
