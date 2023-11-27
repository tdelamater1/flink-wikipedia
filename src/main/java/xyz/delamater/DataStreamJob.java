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

package xyz.delamater;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "192.168.4.130:9092";
        KafkaSource<JsonNode> source = KafkaSource.<JsonNode>builder().
                setBootstrapServers(brokers)
                .setTopics("wikipedia-events")
                .setGroupId("consumer-group-1")
                .setStartingOffsets(OffsetsInitializer.timestamp(1701064800000L))
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        DataStreamSource<JsonNode> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //kafkaSource.print();

        //{"id":1698671021,
        // "domain":"en.wikipedia.org",
        // "namespace":"main namespace",
        // "title":"List of Zimbabwe Twenty20 International cricket records",
        // "timestamp":"2023-11-27T19:19:29Z",
        // "user_name":"Faraz Master",
        // "user_type":"human",
        // "old_length":126239,
        // "new_length":126151}

        DataStream<JsonNode> humanEdits = kafkaSource.filter(new FilterFunction<JsonNode>() {
            @Override
            public boolean filter(JsonNode jsonNode) {
                if (jsonNode.get("domain").asText().equals("en.wikipedia.org")
                        && jsonNode.get("user_type").asText().equals("human")
                        && jsonNode.get("namespace").asText().equals("main namespace")){
                    return true;
                } else {
                    return false;
                }
            }
        });

        // map the json node to a tuple of (user_name, title, 1)
        humanEdits.map(new Tokenizer()).keyBy(0, 1).sum(2).print();


        // Execute program, beginning computation.
        env.execute("Flink Java API learning");
    }

    public static final class Tokenizer implements MapFunction<JsonNode, Tuple3<String, String, Integer>> {
        public Tuple3<String, String, Integer> map(JsonNode jsonNode) {
            return new Tuple3<>(jsonNode.get("user_name").asText(), jsonNode.get("title").asText(), 1);
        }
    }
}
