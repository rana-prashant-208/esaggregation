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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The {@code "shuffle": { "reducers": N }} option of a {@code terms} aggregation.
 * <p>
 * This value is only known to the coordinating node: it is deliberately NOT part of the
 * {@link TermsAggregationBuilder} wire format (see docs/shuffle/PROPOSAL.md section 5.5).
 */
public final class ShuffleConfig implements ToXContentObject {

    public static final ParseField SHUFFLE_FIELD = new ParseField("shuffle");
    public static final ParseField REDUCERS_FIELD = new ParseField("reducers");

    private static final ConstructingObjectParser<ShuffleConfig, Void> PARSER =
        new ConstructingObjectParser<>(SHUFFLE_FIELD.getPreferredName(), false, args -> new ShuffleConfig((int) args[0]));
    static {
        PARSER.declareInt(constructorArg(), REDUCERS_FIELD);
    }

    private final int reducers;

    public ShuffleConfig(int reducers) {
        if (reducers < 1) {
            throw new IllegalArgumentException("[" + SHUFFLE_FIELD.getPreferredName() + "." + REDUCERS_FIELD.getPreferredName()
                + "] must be at least 1. Found [" + reducers + "]");
        }
        this.reducers = reducers;
    }

    public static ShuffleConfig parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public int reducers() {
        return reducers;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REDUCERS_FIELD.getPreferredName(), reducers);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return reducers == ((ShuffleConfig) o).reducers;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(reducers);
    }

    @Override
    public String toString() {
        return "ShuffleConfig{reducers=" + reducers + "}";
    }
}
