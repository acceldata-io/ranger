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

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class TestAdminCentralUserGroupBuilder {

	private static final String ENABLED = "enabled";

	private final ObjectMapper mapper = new ObjectMapper();
	private AdminCentralUserGroupBuilder builder;

	@BeforeEach
	void setUp() {
		builder = new AdminCentralUserGroupBuilder();
		builder.beginSourceForTest();
	}

	@Test
	void disabledObjectMemberIsNotProvisionedFromGroup() {
		builder.mergeUserForTest(user("alice", true), ENABLED);
		builder.mergeUserForTest(user("bob", false), ENABLED);
		ObjectNode alice = memberObject("alice", true);
		ObjectNode bob = memberObject("bob", false);
		builder.mergeGroupForTest(group("analysts", alice, bob), ENABLED);

		assertEquals(Set.of("alice"), builder.getSourceUsersForTest().keySet());
		assertEquals(Set.of("alice"), membersOf("analysts"));
		assertTrue(builder.getSourceGroupsForTest().containsKey("analysts"));
	}

	@Test
	void disabledStringMemberIsNotProvisionedWhenEnabledCheckIsOn() {
		builder.mergeUserForTest(user("alice", true), ENABLED);
		builder.mergeUserForTest(user("bob", false), ENABLED);
		JsonNode alice = mapper.getNodeFactory().textNode("alice");
		JsonNode bob = mapper.getNodeFactory().textNode("bob");
		builder.mergeGroupForTest(group("analysts", alice, bob), ENABLED);

		assertEquals(Set.of("alice"), builder.getSourceUsersForTest().keySet());
		assertEquals(Set.of("alice"), membersOf("analysts"));
		assertFalse(builder.getSourceUsersForTest().containsKey("bob"));
	}

	@Test
	void enabledObjectMemberSeenOnlyOnGroupIsProvisioned() {
		builder.mergeGroupForTest(group("analysts", memberObject("dave", true)), ENABLED);

		assertTrue(builder.getSourceUsersForTest().containsKey("dave"));
		assertEquals(Set.of("dave"), membersOf("analysts"));
	}

	@Test
	void stringMemberSeenOnlyOnGroupIsSkippedWhenEnabledCheckIsOn() {
		builder.mergeGroupForTest(group("analysts", mapper.getNodeFactory().textNode("frank")), ENABLED);

		assertTrue(builder.getSourceUsersForTest().isEmpty());
		assertTrue(membersOf("analysts").isEmpty());
		assertTrue(builder.getSourceGroupsForTest().containsKey("analysts"));
	}

	@Test
	void stringMemberSeenOnlyOnGroupIsProvisionedWhenEnabledCheckIsOff() {
		builder.mergeGroupForTest(group("analysts", mapper.getNodeFactory().textNode("carol")), "");

		assertTrue(builder.getSourceUsersForTest().containsKey("carol"));
		assertEquals(Set.of("carol"), membersOf("analysts"));
	}

	private Set<String> membersOf(String groupName) {
		Set<String> members = builder.getSourceGroupUsersForTest().get(groupName);
		return members == null ? Set.of() : members;
	}

	private ObjectNode user(String username, boolean enabled) {
		ObjectNode user = mapper.createObjectNode();
		user.put("username", username);
		user.put(ENABLED, enabled);
		return user;
	}

	private ObjectNode memberObject(String username, boolean enabled) {
		return user(username, enabled);
	}

	private ObjectNode group(String name, JsonNode... members) {
		ObjectNode group = mapper.createObjectNode();
		group.put("name", name);
		ArrayNode memberArray = mapper.createArrayNode();
		for (JsonNode member : members) {
			memberArray.add(member);
		}
		group.set("members", memberArray);
		return group;
	}
}
