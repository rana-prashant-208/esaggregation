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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.ShuffleConfig;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.shuffle.ShuffleSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Coordinator-side request validation for shuffle mode (docs/shuffle/PROPOSAL.md section 4).
 * All violations are reported as {@link IllegalArgumentException}, which maps to HTTP 400.
 */
public final class ShuffleRequestValidator {

    /** Metric sub-aggregations allowed under a shuffled terms aggregation (section 4.2). */
    static final Set<String> ALLOWED_METRIC_SUB_AGGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        "sum", "min", "max", "avg", "value_count", "stats", "extended_stats", "cardinality", "percentiles")));

    /** Parent pipeline sub-aggregations allowed under a shuffled terms aggregation (section 4.2). */
    static final Set<String> ALLOWED_PIPELINE_SUB_AGGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        "bucket_script", "bucket_selector")));

    /**
     * Value type hints that are compatible with shuffle. The mapped field type itself is only known on the shards in 6.5.4
     * (the coordinator has no mapper service), so the field-type rule is completed shard-side (see code-map.md).
     */
    static final Set<ValueType> ALLOWED_VALUE_TYPES = Collections.unmodifiableSet(EnumSet.of(
        ValueType.STRING, ValueType.LONG, ValueType.DATE));

    private ShuffleRequestValidator() {}

    /**
     * Validates the shuffle part of a search request.
     *
     * @param request         the search request
     * @param crossCluster    whether the search targets at least one remote cluster
     * @param nodes           the nodes of the local cluster, used for the protocol gate
     * @param clusterSettings the dynamic cluster settings
     * @return the shuffled top-level terms aggregation, or {@code null} if the request does not use shuffle
     * @throws IllegalArgumentException if the request uses shuffle in a way that is not allowed
     */
    public static TermsAggregationBuilder validate(SearchRequest request, boolean crossCluster, DiscoveryNodes nodes,
                                                   ClusterSettings clusterSettings) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.aggregations() == null) {
            return null;
        }
        AggregatorFactories.Builder aggs = source.aggregations();

        List<TermsAggregationBuilder> topLevel = new ArrayList<>();
        for (AggregationBuilder agg : aggs.getAggregatorFactories()) {
            if (isShuffled(agg)) {
                topLevel.add((TermsAggregationBuilder) agg);
            }
            String nested = findShuffledDescendant(agg);
            if (nested != null) {
                throw fail("[shuffle] is only supported on a top-level [terms] aggregation, but [" + nested
                    + "] is nested under [" + agg.getName() + "]");
            }
        }
        if (topLevel.isEmpty()) {
            return null;
        }
        if (topLevel.size() > 1) {
            throw fail("only one [terms] aggregation per request may use [shuffle], found [" + topLevel.size() + "]");
        }
        TermsAggregationBuilder terms = topLevel.get(0);
        String name = terms.getName();

        if (clusterSettings.get(ShuffleSettings.SHUFFLE_ENABLED_SETTING) == false) {
            throw fail("[shuffle] on aggregation [" + name + "] is disabled. Set ["
                + ShuffleSettings.SHUFFLE_ENABLED_SETTING.getKey() + "] to true to enable it");
        }
        DiscoveryNode legacy = ShuffleSettings.firstNodeWithoutProtocol(nodes);
        if (legacy != null) {
            throw fail("[shuffle] on aggregation [" + name + "] requires every node to support shuffle protocol ["
                + ShuffleSettings.PROTOCOL_VERSION + "], but node " + legacy + " does not");
        }
        if (request.scroll() != null) {
            throw fail("[shuffle] on aggregation [" + name + "] is not supported for scroll requests");
        }
        if (crossCluster) {
            throw fail("[shuffle] on aggregation [" + name + "] is not supported for cross-cluster search");
        }

        ShuffleConfig shuffle = terms.shuffle();
        int maxReducers = clusterSettings.get(ShuffleSettings.SHUFFLE_MAX_REDUCERS_SETTING);
        if (shuffle.reducers() > maxReducers) {
            throw fail("[shuffle.reducers] on aggregation [" + name + "] must be at most [" + maxReducers + "] ("
                + ShuffleSettings.SHUFFLE_MAX_REDUCERS_SETTING.getKey() + "). Found [" + shuffle.reducers() + "]");
        }

        if (terms.shardSize() != -1) {
            throw fail("[shard_size] cannot be set on aggregation [" + name + "] when [shuffle] is used: shards always send "
                + "every bucket");
        }
        if (terms.shardMinDocCount() != 0) {
            throw fail("[shard_min_doc_count] cannot be set on aggregation [" + name + "] when [shuffle] is used: shards always "
                + "send every bucket");
        }
        if (terms.script() != null) {
            throw fail("[shuffle] on aggregation [" + name + "] does not support scripts");
        }
        if (terms.field() == null) {
            throw fail("[shuffle] on aggregation [" + name + "] requires a [field]");
        }
        if (terms.valueType() != null && ALLOWED_VALUE_TYPES.contains(terms.valueType()) == false) {
            throw fail("[shuffle] on aggregation [" + name + "] only supports keyword and integral/date fields, but [value_type] is ["
                + terms.valueType().getPreferredName() + "]");
        }

        for (AggregationBuilder sub : terms.getSubAggregations()) {
            if (ALLOWED_METRIC_SUB_AGGS.contains(sub.getType()) == false) {
                throw fail("[shuffle] on aggregation [" + name + "] only allows metric sub-aggregations " + sorted(ALLOWED_METRIC_SUB_AGGS)
                    + ", but [" + sub.getName() + "] is of type [" + sub.getType() + "]");
            }
        }
        for (PipelineAggregationBuilder sub : terms.getPipelineAggregations()) {
            if (ALLOWED_PIPELINE_SUB_AGGS.contains(sub.getType()) == false) {
                throw fail("[shuffle] on aggregation [" + name + "] only allows pipeline sub-aggregations "
                    + sorted(ALLOWED_PIPELINE_SUB_AGGS) + ", but [" + sub.getName() + "] is of type [" + sub.getType() + "]");
            }
        }

        for (PipelineAggregationBuilder sibling : aggs.getPipelineAggregatorFactories()) {
            for (String path : sibling.getBucketsPaths()) {
                if (name.equals(firstPathElement(path))) {
                    throw fail("sibling pipeline aggregation [" + sibling.getName() + "] cannot reference aggregation [" + name
                        + "] that uses [shuffle]");
                }
            }
        }
        return terms;
    }

    /**
     * Returns the top-level terms aggregation that requests shuffle, without validating; {@code null} if there is none.
     * Only meaningful after {@link #validate} passed.
     */
    public static TermsAggregationBuilder findShuffled(SearchRequest request) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.aggregations() == null) {
            return null;
        }
        for (AggregationBuilder agg : source.aggregations().getAggregatorFactories()) {
            if (isShuffled(agg)) {
                return (TermsAggregationBuilder) agg;
            }
        }
        return null;
    }

    private static boolean isShuffled(AggregationBuilder agg) {
        return agg instanceof TermsAggregationBuilder && ((TermsAggregationBuilder) agg).shuffle() != null;
    }

    /** Returns the name of the first shuffled terms aggregation strictly below {@code agg}, or {@code null}. */
    private static String findShuffledDescendant(AggregationBuilder agg) {
        for (AggregationBuilder sub : agg.getSubAggregations()) {
            if (isShuffled(sub)) {
                return sub.getName();
            }
            String nested = findShuffledDescendant(sub);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    private static String firstPathElement(String path) {
        try {
            List<AggregationPath.PathElement> elements = AggregationPath.parse(path).getPathElements();
            return elements.isEmpty() ? null : elements.get(0).name;
        } catch (RuntimeException e) {
            // invalid paths are reported by the pipeline's own validation
            return null;
        }
    }

    private static List<String> sorted(Set<String> values) {
        List<String> list = new ArrayList<>(values);
        Collections.sort(list);
        return list;
    }

    private static IllegalArgumentException fail(String message) {
        return new IllegalArgumentException(message);
    }
}
