/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.biz;

import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.Storage;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.services.gcs.RangerGCSConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for GCS IAM translation / merge helpers in {@link ServiceDBStore}.
 * No live GCP calls — Storage is mocked where needed.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestServiceDBStoreGCSIAM {

	private static final String PROJECT_ID = "my-project";
	private static final String BUCKET     = "my-bucket";
	private static final String DEFAULT_BUCKET = "default-bucket";

	private static final Role OBJECT_VIEWER  = Role.of("roles/storage.objectViewer");
	private static final Role OBJECT_CREATOR = Role.of("roles/storage.objectCreator");
	private static final Role BUCKET_READER  = Role.of("roles/storage.legacyBucketReader");

	private ServiceDBStore store;

	@Mock
	private Storage mockStorage;

	@Before
	public void setUp() {
		store = new ServiceDBStore();
	}

	// ── computeGCSIAMBindings ─────────────────────────────────────────────────

	@Test
	public void computeGCSIAMBindings_mapsAllowUsersGroups_skipsDenyAndNonEmailGroups() {
		RangerPolicy policy = policyForBucket(BUCKET);

		RangerPolicyItem allow = new RangerPolicyItem();
		allow.setAccesses(Arrays.asList(
				new RangerPolicyItemAccess("storage.objects.get", true),
				new RangerPolicyItemAccess("storage.objects.create", true)));
		allow.setUsers(Arrays.asList(
				"alice@example.com",
				"sa-full@my-project.iam.gserviceaccount.com",
				"plain-sa"));
		allow.setGroups(Arrays.asList("eng@example.com", "local-group")); // non-email skipped
		allow.setRoles(Collections.singletonList("ranger-data-steward")); // Ranger roles ignored
		policy.setPolicyItems(Collections.singletonList(allow));

		RangerPolicyItem deny = new RangerPolicyItem();
		deny.setAccesses(Collections.singletonList(
				new RangerPolicyItemAccess("storage.objects.delete", true)));
		deny.setUsers(Collections.singletonList("bob@example.com"));
		policy.setDenyPolicyItems(Collections.singletonList(deny));

		Map<Role, Set<Identity>> bindings =
				store.computeGCSIAMBindings(Collections.singletonList(policy), BUCKET, PROJECT_ID);

		Assert.assertEquals(2, bindings.size());
		Assert.assertTrue(bindings.containsKey(OBJECT_VIEWER));
		Assert.assertTrue(bindings.containsKey(OBJECT_CREATOR));
		Assert.assertFalse(bindings.containsKey(Role.of("roles/storage.legacyObjectOwner")));

		Set<Identity> expected = new HashSet<>(Arrays.asList(
				Identity.user("alice@example.com"),
				Identity.serviceAccount("sa-full@my-project.iam.gserviceaccount.com"),
				Identity.serviceAccount("plain-sa@" + PROJECT_ID + ".iam.gserviceaccount.com"),
				Identity.group("eng@example.com")));
		Assert.assertEquals(expected, bindings.get(OBJECT_VIEWER));
		Assert.assertEquals(expected, bindings.get(OBJECT_CREATOR));
	}

	@Test
	public void computeGCSIAMBindings_wildcardApplies_explicitMismatchSkipped() {
		RangerPolicy wildcard = policyForBucket("*");
		RangerPolicyItem item = new RangerPolicyItem();
		item.setAccesses(Collections.singletonList(
				new RangerPolicyItemAccess("storage.buckets.get", true)));
		item.setUsers(Collections.singletonList("viewer@example.com"));
		wildcard.setPolicyItems(Collections.singletonList(item));

		RangerPolicy otherBucket = policyForBucket("other-bucket");
		RangerPolicyItem otherItem = new RangerPolicyItem();
		otherItem.setAccesses(Collections.singletonList(
				new RangerPolicyItemAccess("storage.objects.list", true)));
		otherItem.setUsers(Collections.singletonList("other@example.com"));
		otherBucket.setPolicyItems(Collections.singletonList(otherItem));

		Map<Role, Set<Identity>> bindings = store.computeGCSIAMBindings(
				Arrays.asList(wildcard, otherBucket), BUCKET, PROJECT_ID);

		Assert.assertEquals(1, bindings.size());
		Assert.assertEquals(
				Collections.singleton(Identity.user("viewer@example.com")),
				bindings.get(BUCKET_READER));
	}

	// ── toGCSIdentity (via reflection — three branches) ───────────────────────

	@Test
	public void toGCSIdentity_threeBranches() throws Exception {
		Assert.assertEquals(
				Identity.serviceAccount("sa@proj.iam.gserviceaccount.com"),
				invokeToGCSIdentity("sa@proj.iam.gserviceaccount.com", PROJECT_ID));
		Assert.assertEquals(
				Identity.user("user@example.com"),
				invokeToGCSIdentity("user@example.com", PROJECT_ID));
		Assert.assertEquals(
				Identity.serviceAccount("my-sa@" + PROJECT_ID + ".iam.gserviceaccount.com"),
				invokeToGCSIdentity("my-sa", PROJECT_ID));
	}

	// ── extractAffectedBucketsGCS / gcsPolicyAppliesToBucket ──────────────────

	@Test
	@SuppressWarnings("unchecked")
	public void extractAffectedBucketsGCS_wildcardAndExplicit() throws Exception {
		RangerPolicy explicit = policyForBucket("b1");
		Assert.assertEquals(
				new HashSet<>(Collections.singletonList("b1")),
				(Set<String>) invokeExtractAffectedBuckets(explicit, DEFAULT_BUCKET));

		RangerPolicy wildcard = policyForBucket("*");
		Assert.assertEquals(
				new HashSet<>(Collections.singletonList(DEFAULT_BUCKET)),
				(Set<String>) invokeExtractAffectedBuckets(wildcard, DEFAULT_BUCKET));

		RangerPolicy empty = new RangerPolicy();
		Assert.assertEquals(
				new HashSet<>(Collections.singletonList(DEFAULT_BUCKET)),
				(Set<String>) invokeExtractAffectedBuckets(empty, DEFAULT_BUCKET));
	}

	@Test
	public void gcsPolicyAppliesToBucket_wildcardAndExplicit() throws Exception {
		Assert.assertTrue(invokeAppliesToBucket(policyForBucket("*"), BUCKET));
		Assert.assertTrue(invokeAppliesToBucket(policyForBucket(BUCKET), BUCKET));
		Assert.assertFalse(invokeAppliesToBucket(policyForBucket("other"), BUCKET));
		Assert.assertFalse(invokeAppliesToBucket(new RangerPolicy(), BUCKET));
	}

	// ── buildPreviousGCSPolicies ──────────────────────────────────────────────

	@Test
	@SuppressWarnings("unchecked")
	public void buildPreviousGCSPolicies_excludesCurrent_addsOld() throws Exception {
		RangerPolicy kept = new RangerPolicy();
		kept.setId(1L);
		RangerPolicy current = new RangerPolicy();
		current.setId(2L);
		RangerPolicy old = new RangerPolicy();
		old.setId(2L);
		old.setName("old-snapshot");

		List<RangerPolicy> servicePolicies = Arrays.asList(kept, current);
		List<RangerPolicy> previous =
				(List<RangerPolicy>) invokeBuildPrevious(servicePolicies, current, old);

		Assert.assertEquals(2, previous.size());
		Assert.assertTrue(previous.contains(kept));
		Assert.assertTrue(previous.contains(old));
		Assert.assertFalse(previous.contains(current));
	}

	// ── applyGCSIAMPolicy ─────────────────────────────────────────────────────

	@Test
	public void applyGCSIAMPolicy_preservesUnrelated_passesEtag_skipsNoOp() {
		Identity rangerUser   = Identity.user("ranger@example.com");
		Identity externalUser = Identity.user("external@example.com");
		String etag = "etag-abc";

		Map<Role, Set<Identity>> existing = new HashMap<>();
		existing.put(OBJECT_VIEWER, new HashSet<>(Arrays.asList(rangerUser, externalUser)));
		existing.put(BUCKET_READER, new HashSet<>(Collections.singletonList(externalUser)));

		Policy existingPolicy = Policy.newBuilder().setBindings(existing).setEtag(etag).build();
		Mockito.when(mockStorage.getIamPolicy(BUCKET)).thenReturn(existingPolicy);

		// Replace rangerUser with a new member; leave externalUser alone.
		Map<Role, Set<Identity>> previous = new HashMap<>();
		previous.put(OBJECT_VIEWER, Collections.singleton(rangerUser));
		Map<Role, Set<Identity>> current = new HashMap<>();
		current.put(OBJECT_VIEWER, Collections.singleton(Identity.user("new@example.com")));

		store.applyGCSIAMPolicy(mockStorage, BUCKET, previous, current);

		ArgumentCaptor<Policy> captor = ArgumentCaptor.forClass(Policy.class);
		Mockito.verify(mockStorage).setIamPolicy(Mockito.eq(BUCKET), captor.capture());
		Policy written = captor.getValue();
		Assert.assertEquals(etag, written.getEtag());
		Assert.assertTrue(written.getBindings().get(OBJECT_VIEWER).contains(externalUser));
		Assert.assertTrue(written.getBindings().get(OBJECT_VIEWER).contains(Identity.user("new@example.com")));
		Assert.assertFalse(written.getBindings().get(OBJECT_VIEWER).contains(rangerUser));
		Assert.assertEquals(Collections.singleton(externalUser), written.getBindings().get(BUCKET_READER));

		// No-op: previous == current already reflected in live policy → skip setIamPolicy.
		Mockito.reset(mockStorage);
		Map<Role, Set<Identity>> live = new HashMap<>();
		live.put(OBJECT_VIEWER, new HashSet<>(Collections.singletonList(externalUser)));
		Mockito.when(mockStorage.getIamPolicy(BUCKET))
				.thenReturn(Policy.newBuilder().setBindings(live).setEtag(etag).build());

		Map<Role, Set<Identity>> same = new HashMap<>();
		same.put(OBJECT_VIEWER, Collections.singleton(externalUser));
		store.applyGCSIAMPolicy(mockStorage, BUCKET, same, same);

		Mockito.verify(mockStorage, Mockito.never()).setIamPolicy(Mockito.anyString(), Mockito.any(Policy.class));
	}

	// ── helpers ───────────────────────────────────────────────────────────────

	private static RangerPolicy policyForBucket(String bucketName) {
		RangerPolicy policy = new RangerPolicy();
		policy.setName("gcs-policy-" + bucketName);
		Map<String, RangerPolicyResource> resources = new HashMap<>();
		resources.put(RangerGCSConstants.BUCKET,
				new RangerPolicyResource(Collections.singletonList(bucketName), false, false));
		policy.setResources(resources);
		return policy;
	}

	private Identity invokeToGCSIdentity(String user, String projectId) throws Exception {
		Method m = ServiceDBStore.class.getDeclaredMethod("toGCSIdentity", String.class, String.class);
		m.setAccessible(true);
		return (Identity) m.invoke(store, user, projectId);
	}

	private Object invokeExtractAffectedBuckets(RangerPolicy policy, String defaultBucket) throws Exception {
		Method m = ServiceDBStore.class.getDeclaredMethod("extractAffectedBucketsGCS", RangerPolicy.class, String.class);
		m.setAccessible(true);
		return m.invoke(store, policy, defaultBucket);
	}

	private boolean invokeAppliesToBucket(RangerPolicy policy, String bucket) throws Exception {
		Method m = ServiceDBStore.class.getDeclaredMethod("gcsPolicyAppliesToBucket", RangerPolicy.class, String.class);
		m.setAccessible(true);
		return (Boolean) m.invoke(store, policy, bucket);
	}

	private Object invokeBuildPrevious(List<RangerPolicy> servicePolicies, RangerPolicy current, RangerPolicy old)
			throws Exception {
		Method m = ServiceDBStore.class.getDeclaredMethod(
				"buildPreviousGCSPolicies", List.class, RangerPolicy.class, RangerPolicy.class);
		m.setAccessible(true);
		return m.invoke(store, servicePolicies, current, old);
	}
}
