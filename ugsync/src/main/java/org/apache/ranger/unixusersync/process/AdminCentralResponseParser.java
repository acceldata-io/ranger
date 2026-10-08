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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * JSON navigation helpers for Admin Central (and similar) REST payloads.
 */
public final class AdminCentralResponseParser {

	private static final Logger LOG = LoggerFactory.getLogger(AdminCentralResponseParser.class);

	private static final ObjectMapper MAPPER = new ObjectMapper();

	/** Field names already reported as missing, so a bad path does not warn once per user. */
	private static final Set<String> WARNED_MISSING_ENABLED_FIELDS = ConcurrentHashMap.newKeySet();

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

	/**
	 * Walk {@code dotPath} from {@code root}. A blank path returns {@code root}.
	 * Leading and trailing dots are ignored ({@code .data} and {@code data.} are {@code data}).
	 * An empty segment in the middle ({@code a..b}) is rejected.
	 */
	public static JsonNode navigate(JsonNode root, String dotPath) {
		if (root == null || root.isNull() || root.isMissingNode()) {
			return null;
		}
		if (StringUtils.isBlank(dotPath)) {
			return root;
		}
		String path = stripEdgeDots(dotPath.trim());
		if (path.isEmpty()) {
			throw new IllegalArgumentException(
					"Invalid JSON path \"" + dotPath + "\": path contains no field names");
		}
		JsonNode n = root;
		for (String part : path.split("\\.", -1)) {
			if (part.isEmpty()) {
				throw new IllegalArgumentException(
						"Invalid JSON path \"" + dotPath + "\": empty segment (\"..\"); "
								+ "check ranger.usersync.admincentral users/groups array path");
			}
			if (n == null || n.isNull() || n.isMissingNode()) {
				return null;
			}
			n = n.get(part);
		}
		return n;
	}

	/** Drop dots that only pad the start or end of a path. Interior dots are left in place. */
	private static String stripEdgeDots(String path) {
		int start = 0;
		int end = path.length();
		while (start < end && path.charAt(start) == '.') {
			start++;
		}
		while (end > start && path.charAt(end - 1) == '.') {
			end--;
		}
		return path.substring(start, end);
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

	/**
	 * A blank {@code enabledFieldName} means the operator did not opt into the check, so every user is enabled.
	 * When the field is set, a missing or null value is not enabled: a typo or renamed payload must not sync
	 * disabled users.
	 */
	public static boolean isEffectivelyEnabled(JsonNode user, String enabledFieldName) {
		if (StringUtils.isBlank(enabledFieldName)) {
			return true;
		}
		String field = enabledFieldName.trim();
		if (user == null || user.isNull() || user.isMissingNode()) {
			return false;
		}
		JsonNode en = user.get(field);
		if (en == null || en.isNull() || en.isMissingNode()) {
			if (WARNED_MISSING_ENABLED_FIELDS.add(field)) {
				LOG.warn(
						"User payload has no \"{}\" field (ranger.usersync.admincentral.user.enabled.field); treating the user as not enabled",
						field);
			}
			return false;
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
