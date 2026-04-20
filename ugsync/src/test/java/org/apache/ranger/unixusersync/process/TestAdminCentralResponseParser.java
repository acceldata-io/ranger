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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class TestAdminCentralResponseParser {

	private final ObjectMapper mapper = new ObjectMapper();

	@Test
	void navigateEmptyPathReturnsRoot() throws Exception {
		ObjectNode root = mapper.createObjectNode();
		root.put("a", 1);
		assertEquals(root, AdminCentralResponseParser.navigate(root, ""));
		assertEquals(root, AdminCentralResponseParser.navigate(root, "   "));
	}

	@Test
	void navigateDotPath() throws Exception {
		ObjectNode root = mapper.createObjectNode();
		ObjectNode inner = mapper.createObjectNode();
		ArrayNode arr = mapper.createArrayNode();
		arr.add("x");
		inner.set("items", arr);
		root.set("data", inner);
		assertEquals(arr, AdminCentralResponseParser.navigate(root, "data.items"));
	}

	@Test
	void asArrayAcceptsJsonArray() throws Exception {
		ArrayNode arr = mapper.createArrayNode();
		arr.add("u1");
		assertNotNull(AdminCentralResponseParser.asArray(arr));
	}

	@Test
	void asArrayRejectsObject() throws Exception {
		ObjectNode obj = mapper.createObjectNode();
		assertNull(AdminCentralResponseParser.asArray(obj));
	}

	@Test
	void isEffectivelyEnabledHonorsField() throws Exception {
		ObjectNode u = mapper.createObjectNode();
		u.put("enabled", false);
		assertFalse(AdminCentralResponseParser.isEffectivelyEnabled(u, "enabled"));
		u.put("enabled", true);
		assertTrue(AdminCentralResponseParser.isEffectivelyEnabled(u, "enabled"));
	}

	@Test
	void isEffectivelyEnabledMissingFieldMeansTrue() throws Exception {
		ObjectNode u = mapper.createObjectNode();
		assertTrue(AdminCentralResponseParser.isEffectivelyEnabled(u, "enabled"));
	}

	@Test
	void parseSpringPageContent() throws Exception {
		String json =
				"{\"content\":[{\"username\":\"alice\",\"groups\":[\"g1\"]}],\"totalElements\":1}";
		ObjectNode root = (ObjectNode) mapper.readTree(json);
		ArrayNode content = AdminCentralResponseParser.asArray(AdminCentralResponseParser.navigate(root, "content"));
		assertNotNull(content);
		assertEquals(1, content.size());
		assertEquals("alice", content.get(0).get("username").asText());
	}
}
