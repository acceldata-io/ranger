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

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * JSON navigation helpers for Admin Central (and similar) REST payloads.
 */
public final class AdminCentralResponseParser {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	/** Admin Central application-level failure code (see XDP CP identity APIs). */
	public static final int ERROR_CODE_FAILURE = 1;

	private AdminCentralResponseParser() {
	}

	/**
	 * Returns the Admin Central {@code message} when the JSON envelope indicates failure
	 * ({@code errorCode == 1}); otherwise {@code null}.
	 */
	public static String apiErrorMessageIfPresent(JsonNode root) {
		if (root == null || root.isNull() || !root.isObject()) {
			return null;
		}
		JsonNode errorCode = root.get("errorCode");
		if (errorCode == null || errorCode.isNull() || !errorCode.isNumber()
				|| errorCode.asInt() != ERROR_CODE_FAILURE) {
			return null;
		}
		String message = textOrNull(root.get("message"));
		if (StringUtils.isNotBlank(message)) {
			return message.trim();
		}
		String detailed = textOrNull(root.get("detailedMessage"));
		if (StringUtils.isNotBlank(detailed)) {
			return detailed.trim();
		}
		return "Admin Central API returned errorCode=" + ERROR_CODE_FAILURE;
	}

	/**
	 * Same as {@link #apiErrorMessageIfPresent(JsonNode)} for a raw JSON body string.
	 * Returns {@code null} if the body is blank or not a JSON object error envelope.
	 */
	public static String apiErrorMessageIfPresent(String jsonBody) {
		if (StringUtils.isBlank(jsonBody)) {
			return null;
		}
		try {
			return apiErrorMessageIfPresent(MAPPER.readTree(jsonBody));
		} catch (Exception e) {
			return null;
		}
	}

	public static JsonNode navigate(JsonNode root, String dotPath) {
		if (root == null || root.isNull() || root.isMissingNode()) {
			return null;
		}
		if (StringUtils.isBlank(dotPath)) {
			return root;
		}
		JsonNode n = root;
		for (String part : dotPath.split("\\.")) {
			if (n == null || n.isNull() || n.isMissingNode()) {
				return null;
			}
			n = n.get(part);
		}
		return n;
	}

	public static ArrayNode asArray(JsonNode node) {
		if (node == null || node.isNull() || node.isMissingNode()) {
			return null;
		}
		if (node.isArray()) {
			return (ArrayNode) node;
		}
		return null;
	}

	public static boolean isEffectivelyEnabled(JsonNode user, String enabledFieldName) {
		if (user == null || StringUtils.isBlank(enabledFieldName)) {
			return true;
		}
		JsonNode en = user.get(enabledFieldName.trim());
		if (en == null || en.isNull() || en.isMissingNode()) {
			return true;
		}
		if (en.isBoolean()) {
			return en.booleanValue();
		}
		if (en.isTextual()) {
			return Boolean.parseBoolean(en.asText());
		}
		if (en.isNumber()) {
			return en.intValue() != 0;
		}
		return true;
	}

	public static String textOrNull(JsonNode node) {
		if (node == null || node.isNull() || node.isMissingNode()) {
			return null;
		}
		if (node.isTextual()) {
			return node.asText();
		}
		if (node.isNumber()) {
			return node.asText();
		}
		return node.asText();
	}
}
