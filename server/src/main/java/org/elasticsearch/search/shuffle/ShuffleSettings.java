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
package org.elasticsearch.search.shuffle;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Settings and protocol constants for the shuffle-based terms aggregation (docs/shuffle/PROPOSAL.md).
 */
public final class ShuffleSettings {

    private ShuffleSettings() {}

    /** Master switch. */
    public static final Setting<Boolean> SHUFFLE_ENABLED_SETTING =
        Setting.boolSetting("search.shuffle.enabled", false, Property.Dynamic, Property.NodeScope);

    /** Upper bound on {@code shuffle.reducers}. */
    public static final Setting<Integer> SHUFFLE_MAX_REDUCERS_SETTING =
        Setting.intSetting("search.shuffle.max_reducers", 16, 1, Property.Dynamic, Property.NodeScope);

    /** Reducer state not finalized within this time is freed. */
    public static final Setting<TimeValue> SHUFFLE_STATE_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("search.shuffle.state_timeout", TimeValue.timeValueMinutes(5), Property.Dynamic,
            Property.NodeScope);

    /**
     * Multiplier from serialized partial size to estimated heap, for breaker accounting. Measured in M2
     * ({@code ShuffleServiceTests#testHeapEstimateFactorCoversRealisticPartials}): a decoded keyword terms partial with a sum
     * sub-aggregation takes about 7x its serialized size on heap (6.9x at 20k buckets, 7.1x at 1k, 8.0x at 10).
     */
    public static final Setting<Double> SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING =
        Setting.doubleSetting("search.shuffle.heap_estimate_factor", 8.0d, 1.0d, Property.Dynamic, Property.NodeScope);

    /** Node attribute every fork node advertises (section 7.1). */
    public static final String PROTOCOL_ATTRIBUTE = "search_shuffle_protocol";

    /** Current shuffle protocol version. Bump on any change to the partition function or shuffle transport messages. */
    public static final String PROTOCOL_VERSION = "1";

    /** Full setting key of the protocol attribute. */
    public static final String PROTOCOL_ATTRIBUTE_SETTING_KEY = "node.attr." + PROTOCOL_ATTRIBUTE;

    /**
     * Returns {@code settings} with the shuffle protocol node attribute set. The value is forced: a node cannot claim a
     * protocol version it does not implement.
     */
    public static Settings withProtocolAttribute(Settings settings) {
        if (PROTOCOL_VERSION.equals(settings.get(PROTOCOL_ATTRIBUTE_SETTING_KEY))) {
            return settings;
        }
        return Settings.builder().put(settings).put(PROTOCOL_ATTRIBUTE_SETTING_KEY, PROTOCOL_VERSION).build();
    }

    /** Returns the first node that does not advertise the current shuffle protocol, or {@code null} if all do. */
    public static DiscoveryNode firstNodeWithoutProtocol(DiscoveryNodes nodes) {
        for (DiscoveryNode node : nodes) {
            if (PROTOCOL_VERSION.equals(node.getAttributes().get(PROTOCOL_ATTRIBUTE)) == false) {
                return node;
            }
        }
        return null;
    }
}
