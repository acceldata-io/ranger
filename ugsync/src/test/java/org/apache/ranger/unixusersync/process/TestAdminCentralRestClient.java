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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

class TestAdminCentralRestClient {

	@Test
	void xdpAuthRequiresApiKeysFromEnvLookup() {
		Map<String, String> env =
				AdminCentralRestClient.getenvMapForTesting(
						"API_ACCESS_KEY", "access-key-value", "API_SECRET_KEY", "secret-value");
		new AdminCentralRestClient(1000, 1000, "xdp", null, null, "", env::get);
	}

	@Test
	void xdpAuthAcceptsDpPrefixedKeys() {
		Map<String, String> env =
				AdminCentralRestClient.getenvMapForTesting("DP_ACCESS_KEY", "k", "DP_SECRET_KEY", "s");
		new AdminCentralRestClient(1000, 1000, "xdp", null, null, "", env::get);
	}

	@Test
	void akskAliasSameAsXdp() {
		Map<String, String> env =
				AdminCentralRestClient.getenvMapForTesting(
						"API_ACCESS_KEY", "a", "API_SECRET_KEY", "b");
		new AdminCentralRestClient(1000, 1000, "aksk", null, null, "", env::get);
	}

	@Test
	void xdpAuthFailsWhenKeysMissing() {
		final Map<String, String> empty = Collections.emptyMap();
		IllegalArgumentException ex =
				assertThrows(
						IllegalArgumentException.class,
						() ->
								new AdminCentralRestClient(1000, 1000, "xdp", null, null, "", empty::get));
		assertTrue(ex.getMessage().contains("API_ACCESS_KEY"));
	}
}
