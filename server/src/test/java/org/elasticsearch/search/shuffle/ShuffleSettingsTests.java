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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ShuffleSettingsTests extends ESTestCase {

    public void testDefaults() {
        assertFalse(ShuffleSettings.SHUFFLE_ENABLED_SETTING.get(Settings.EMPTY));
        assertThat(ShuffleSettings.SHUFFLE_MAX_REDUCERS_SETTING.get(Settings.EMPTY), equalTo(16));
        assertThat(ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING.get(Settings.EMPTY), equalTo(TimeValue.timeValueMinutes(5)));
    }

    public void testRegisteredAndDynamic() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertTrue(clusterSettings.isDynamicSetting(ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey()));
        assertTrue(clusterSettings.isDynamicSetting(ShuffleSettings.SHUFFLE_MAX_REDUCERS_SETTING.getKey()));
        assertTrue(clusterSettings.isDynamicSetting(ShuffleSettings.SHUFFLE_STATE_TIMEOUT_SETTING.getKey()));
        assertTrue(clusterSettings.isDynamicSetting(ShuffleSettings.SHUFFLE_HEAP_ESTIMATE_FACTOR_SETTING.getKey()));
        expectThrows(IllegalArgumentException.class,
            () -> ShuffleSettings.SHUFFLE_MAX_REDUCERS_SETTING.get(Settings.builder().put("search.shuffle.max_reducers", 0).build()));
    }

    public void testWithProtocolAttribute() {
        Settings settings = ShuffleSettings.withProtocolAttribute(Settings.builder().put("node.attr.rack", "r1").build());
        assertThat(Node.NODE_ATTRIBUTES.getAsMap(settings).get(ShuffleSettings.PROTOCOL_ATTRIBUTE),
            equalTo(ShuffleSettings.PROTOCOL_VERSION));
        assertThat(settings.get("node.attr.rack"), equalTo("r1"));

        // a user-provided value cannot claim another protocol version
        Settings forced = ShuffleSettings.withProtocolAttribute(
            Settings.builder().put(ShuffleSettings.PROTOCOL_ATTRIBUTE_SETTING_KEY, "99").build());
        assertThat(forced.get(ShuffleSettings.PROTOCOL_ATTRIBUTE_SETTING_KEY), equalTo(ShuffleSettings.PROTOCOL_VERSION));
    }
}
