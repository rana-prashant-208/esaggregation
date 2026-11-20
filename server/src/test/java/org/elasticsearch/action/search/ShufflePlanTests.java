/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShufflePlanTests extends ESTestCase {

    static ShufflePlan randomPlan() {
        int numReducers = randomIntBetween(1, 16);
        List<String> reducers = new ArrayList<>();
        for (int i = 0; i < numReducers; i++) {
            reducers.add(UUIDs.randomBase64UUID(random()));
        }
        if (randomBoolean()) {
            return ShufflePlan.identity(UUIDs.randomBase64UUID(random()), randomAlphaOfLengthBetween(1, 20), reducers,
                UUIDs.randomBase64UUID(random()));
        }
        int numPartitions = randomIntBetween(1, 256);
        int[] mapping = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            mapping[i] = randomIntBetween(0, numReducers - 1);
        }
        return new ShufflePlan(UUIDs.randomBase64UUID(random()), randomRealisticUnicodeOfLengthBetween(1, 20), reducers,
            numPartitions, mapping, UUIDs.randomBase64UUID(random()));
    }

    public void testEncodeDecodeRoundTrip() {
        for (int i = 0; i < 50; i++) {
            ShufflePlan plan = randomPlan();
            String encoded = plan.encode();
            ShufflePlan decoded = ShufflePlan.decode(encoded);
            assertEquals(plan, decoded);
            assertEquals(plan.hashCode(), decoded.hashCode());
            assertEquals(encoded, decoded.encode());
        }
    }

    public void testEncodingIsHeaderSafe() {
        String encoded = randomPlan().encode();
        assertTrue(encoded, encoded.matches("[A-Za-z0-9_-]+"));
    }

    public void testIdentity() {
        List<String> reducers = new ArrayList<>();
        reducers.add("a");
        reducers.add("b");
        reducers.add("c");
        ShufflePlan plan = ShufflePlan.identity("id", "agg", reducers, "coord");
        assertThat(plan.numPartitions(), equalTo(3));
        for (int p = 0; p < 3; p++) {
            assertThat(plan.reducerForPartition(p), equalTo(p));
        }
    }

    public void testThroughThreadContext() {
        ShufflePlan plan = randomPlan();
        int attempt = randomIntBetween(1, 10);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertThat(ShufflePlan.fromThreadContext(threadContext), nullValue());
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(ShufflePlan.PLAN_HEADER, plan.encode());
            threadContext.putHeader(ShufflePlan.ATTEMPT_HEADER, Integer.toString(attempt));
            assertEquals(plan, ShufflePlan.fromThreadContext(threadContext));
            assertEquals(attempt, ShufflePlan.attemptFromThreadContext(threadContext));
        }
        assertThat(ShufflePlan.fromThreadContext(threadContext), nullValue());
    }

    public void testMissingAttempt() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        expectThrows(IllegalStateException.class, () -> ShufflePlan.attemptFromThreadContext(threadContext));
    }

    public void testDecodeGarbage() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ShufflePlan.decode("!!not base64!!"));
        assertThat(e.getMessage(), containsString(ShufflePlan.PLAN_HEADER));
        String truncated = randomPlan().encode();
        expectThrows(IllegalArgumentException.class, () -> ShufflePlan.decode(truncated.substring(0, truncated.length() / 2)));
    }

    public void testInvalidPlans() {
        List<String> reducers = new ArrayList<>();
        reducers.add("a");
        expectThrows(IllegalArgumentException.class, () -> new ShufflePlan("id", "agg", new ArrayList<>(), 1, new int[]{0}, "c"));
        expectThrows(IllegalArgumentException.class, () -> new ShufflePlan("id", "agg", reducers, 0, new int[0], "c"));
        expectThrows(IllegalArgumentException.class, () -> new ShufflePlan("id", "agg", reducers, 2, new int[]{0}, "c"));
        expectThrows(IllegalArgumentException.class, () -> new ShufflePlan("id", "agg", reducers, 1, new int[]{1}, "c"));
    }
}
