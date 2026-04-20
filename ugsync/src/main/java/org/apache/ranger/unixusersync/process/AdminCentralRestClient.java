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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal HTTP GET client for Admin Central JSON APIs (Keycloak-backed user directory).
 *
 * <p>For XDP-style authentication (same headers as {@code RangerAdminXdpRESTClient} in Gravitino),
 * set {@code ranger.usersync.admincentral.auth.type} to {@code xdp} (or {@code aksk}) and provide
 * credentials via environment variables {@code API_ACCESS_KEY} / {@code API_SECRET_KEY}, with
 * fallback to {@code DP_ACCESS_KEY} / {@code DP_SECRET_KEY}.
 */
final class AdminCentralRestClient {

	private static final Logger LOG = LoggerFactory.getLogger(AdminCentralRestClient.class);

	/** Matches Gravitino {@code AuthConstants.HTTP_HEADER_ACCESS_KEY}. */
	private static final String HEADER_ACCESS_KEY = "accessKey";

	/** Matches Gravitino {@code AuthConstants.HTTP_HEADER_SECRET_KEY}. */
	private static final String HEADER_SECRET_KEY = "secretKey";

	private static final String ENV_API_ACCESS_KEY = "API_ACCESS_KEY";
	private static final String ENV_API_SECRET_KEY = "API_SECRET_KEY";
	private static final String ENV_DP_ACCESS_KEY = "DP_ACCESS_KEY";
	private static final String ENV_DP_SECRET_KEY = "DP_SECRET_KEY";

	private final int connectTimeoutMs;
	private final int readTimeoutMs;
	private final String authType;
	private final String basicAuthHeader;
	private final String bearerToken;
	private final String xdpAccessKey;
	private final String xdpSecretKey;

	AdminCentralRestClient(
			int connectTimeoutMs,
			int readTimeoutMs,
			String authType,
			String username,
			String password,
			String bearerToken) {
		this(connectTimeoutMs, readTimeoutMs, authType, username, password, bearerToken, System::getenv);
	}

	/**
	 * @param getenv env var lookup; tests may supply a fixed map, production uses {@link
	 *     System#getenv(String)}
	 */
	AdminCentralRestClient(
			int connectTimeoutMs,
			int readTimeoutMs,
			String authType,
			String username,
			String password,
			String bearerToken,
			Function<String, String> getenv) {
		this.connectTimeoutMs = connectTimeoutMs;
		this.readTimeoutMs = readTimeoutMs;
		this.authType = authType == null ? "none" : authType.trim().toLowerCase();
		this.bearerToken = StringUtils.trimToEmpty(bearerToken);
		Function<String, String> getenvFn = getenv != null ? getenv : System::getenv;
		if ("basic".equals(this.authType) && StringUtils.isNotBlank(username)) {
			String raw = username + ":" + StringUtils.defaultString(password);
			this.basicAuthHeader = "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
		} else {
			this.basicAuthHeader = null;
		}
		if (isXdpAkskAuth(this.authType)) {
			String access = firstNonBlankEnv(getenvFn, ENV_API_ACCESS_KEY, ENV_DP_ACCESS_KEY);
			String secret =
					firstNonBlankEnv(getenvFn, ENV_API_SECRET_KEY, ENV_DP_SECRET_KEY);
			if (StringUtils.isBlank(access) || StringUtils.isBlank(secret)) {
				throw new IllegalArgumentException(
						"Admin Central auth.type=xdp (or aksk) requires API_ACCESS_KEY and API_SECRET_KEY "
								+ "(or DP_ACCESS_KEY and DP_SECRET_KEY) environment variables.");
			}
			this.xdpAccessKey = access;
			this.xdpSecretKey = secret;
			LOG.info(
					"Admin Central REST client using XDP AK/SK headers from environment. accessKey={}, secretKey={}",
					maskSecret(xdpAccessKey),
					maskSecret(xdpSecretKey));
		} else {
			this.xdpAccessKey = null;
			this.xdpSecretKey = null;
		}
	}

	String getJson(String urlString) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("GET {}", urlString);
		}
		URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setConnectTimeout(connectTimeoutMs);
		conn.setReadTimeout(readTimeoutMs);
		conn.setRequestProperty("Accept", "application/json");
		if ("basic".equals(authType) && basicAuthHeader != null) {
			conn.setRequestProperty("Authorization", basicAuthHeader);
		}
		if ("bearer".equals(authType) && StringUtils.isNotBlank(bearerToken)) {
			conn.setRequestProperty("Authorization", "Bearer " + bearerToken);
		}
		if (isXdpAkskAuth(authType)) {
			conn.setRequestProperty(HEADER_ACCESS_KEY, xdpAccessKey);
			conn.setRequestProperty(HEADER_SECRET_KEY, xdpSecretKey);
		}
		conn.connect();
		int code = conn.getResponseCode();
		InputStream stream = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		if (stream != null) {
			IOUtils.copy(stream, buffer);
		}
		String body = buffer.toString(StandardCharsets.UTF_8.name());
		if (code < 200 || code >= 300) {
			throw new IOException("HTTP " + code + " from " + urlString + ": " + body);
		}
		return body;
	}

	private static boolean isXdpAkskAuth(String type) {
		return "xdp".equals(type) || "aksk".equals(type);
	}

	static Map<String, String> getenvMapForTesting(String... keyValuePairs) {
		if (keyValuePairs.length % 2 != 0) {
			throw new IllegalArgumentException("Even number of key/value strings required");
		}
		Map<String, String> m = new HashMap<>();
		for (int i = 0; i < keyValuePairs.length; i += 2) {
			m.put(keyValuePairs[i], keyValuePairs[i + 1]);
		}
		return m;
	}

	private static String firstNonBlankEnv(Function<String, String> getenv, String... names) {
		if (names == null) {
			return null;
		}
		for (String n : names) {
			if (StringUtils.isBlank(n)) {
				continue;
			}
			String v = getenv.apply(n);
			if (StringUtils.isNotBlank(v)) {
				return v.trim();
			}
		}
		return null;
	}

	private static String maskSecret(String value) {
		if (StringUtils.isBlank(value)) {
			return "<empty>";
		}
		String trimmed = value.trim();
		int visible = Math.min(4, trimmed.length());
		String suffix = trimmed.substring(trimmed.length() - visible);
		return "****" + suffix;
	}
}
