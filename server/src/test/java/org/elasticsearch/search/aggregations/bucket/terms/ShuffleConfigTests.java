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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ShuffleConfigTests extends ESTestCase {

    private final SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private TermsAggregationBuilder parseSingle(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            AggregatorFactories.Builder builder = AggregatorFactories.parseAggregators(parser);
            AggregationBuilder agg = builder.getAggregatorFactories().iterator().next();
            assertThat(agg, instanceOf(TermsAggregationBuilder.class));
            return (TermsAggregationBuilder) agg;
        }
    }

    public void testParse() throws IOException {
        TermsAggregationBuilder terms = parseSingle(
            "{\"top_users\":{\"terms\":{\"field\":\"user\",\"size\":100,\"shuffle\":{\"reducers\":5}}}}");
        assertThat(terms.shuffle(), equalTo(new ShuffleConfig(5)));
        assertThat(terms.size(), equalTo(100));
    }

    public void testParseAbsentIsNull() throws IOException {
        TermsAggregationBuilder terms = parseSingle("{\"t\":{\"terms\":{\"field\":\"user\"}}}");
        assertThat(terms.shuffle(), nullValue());
    }

    public void testParseRejectsUnknownField() {
        Exception e = expectThrows(Exception.class,
            () -> parseSingle("{\"t\":{\"terms\":{\"field\":\"user\",\"shuffle\":{\"reducers\":2,\"bogus\":1}}}}"));
        assertThat(org.elasticsearch.ExceptionsHelper.stackTrace(e), containsString("bogus"));
    }

    public void testParseRejectsMissingReducers() {
        expectThrows(Exception.class, () -> parseSingle("{\"t\":{\"terms\":{\"field\":\"user\",\"shuffle\":{}}}}"));
    }

    public void testReducersMustBePositive() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ShuffleConfig(randomIntBetween(-10, 0)));
        assertThat(e.getMessage(), containsString("must be at least 1"));
    }

    public void testXContentRoundTrip() throws IOException {
        TermsAggregationBuilder original = new TermsAggregationBuilder("top_users", null)
            .field("user")
            .size(randomIntBetween(1, 1000))
            .shuffle(new ShuffleConfig(randomIntBetween(1, 64)));
        original.subAggregation(new SumAggregationBuilder("bytes").field("bytes"));
        AggregatorFactories.Builder wrapper = new AggregatorFactories.Builder().addAggregator(original);
        String json = Strings.toString(wrapper);
        assertThat(json, containsString("\"shuffle\":{\"reducers\":" + original.shuffle().reducers() + "}"));
        TermsAggregationBuilder parsed = parseSingle(json);
        assertEquals(original, parsed);
        assertEquals(original.hashCode(), parsed.hashCode());
    }

    public void testEqualsIncludesShuffle() {
        TermsAggregationBuilder a = new TermsAggregationBuilder("t", null).field("user");
        TermsAggregationBuilder b = new TermsAggregationBuilder("t", null).field("user").shuffle(new ShuffleConfig(3));
        assertNotEquals(a, b);
        assertNotEquals(b, new TermsAggregationBuilder("t", null).field("user").shuffle(new ShuffleConfig(4)));
        assertEquals(b, new TermsAggregationBuilder("t", null).field("user").shuffle(new ShuffleConfig(3)));
    }

    /** Rewrite goes through shallowCopy; shuffle must survive it. */
    public void testShallowCopyKeepsShuffle() {
        TermsAggregationBuilder terms = new TermsAggregationBuilder("t", null).field("user").shuffle(new ShuffleConfig(7));
        AggregationBuilder copy = terms.shallowCopy(new AggregatorFactories.Builder(), Collections.emptyMap());
        assertThat(((TermsAggregationBuilder) copy).shuffle(), equalTo(new ShuffleConfig(7)));
    }

    /**
     * The stream format must be byte-for-byte identical to stock 6.5.4: shuffle is never serialized, so a builder with
     * shuffle writes exactly the bytes of the same builder without it, and reads back without it.
     */
    public void testStreamSerializationUnchanged() throws IOException {
        TermsAggregationBuilder plain = new TermsAggregationBuilder("top_users", null).field("user").size(randomIntBetween(1, 500));
        TermsAggregationBuilder shuffled = new TermsAggregationBuilder("top_users", null).field("user").size(plain.size())
            .shuffle(new ShuffleConfig(randomIntBetween(1, 16)));

        BytesReference plainBytes = serialize(plain);
        BytesReference shuffledBytes = serialize(shuffled);
        assertEquals(plainBytes, shuffledBytes);

        NamedWriteableRegistry registry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        try (StreamInput in = new NamedWriteableAwareStreamInput(shuffledBytes.streamInput(), registry)) {
            TermsAggregationBuilder read = new TermsAggregationBuilder(in);
            assertThat(read.shuffle(), nullValue());
            assertEquals(plain, read);
        }
    }

    private static BytesReference serialize(TermsAggregationBuilder builder) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            builder.writeTo(out);
            return out.bytes();
        }
    }
}
