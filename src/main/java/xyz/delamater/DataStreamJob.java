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
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class DataStreamJob {

    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        String brokers = "192.168.4.130:9092";
        KafkaSource<EditEvent> source = KafkaSource.<EditEvent>builder().
                setBootstrapServers(brokers)
                .setTopics("wikipedia-events")
                .setGroupId("consumer-group-1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

//        String mongoUser = System.getenv("MONGO_USER");
//        String mongoPass = System.getenv("MONGO_PASS");
//
//        MongoSink<String> sink = MongoSink.<String>builder()
//                .setUri("mongodb://" + mongoUser + ":" + mongoPass + "@192.168.4.100:27017/wikipedia")
//                .setDatabase("wikipedia")
//                .setCollection("edits")
//                .setBatchSize(1000)
//                .setBatchIntervalMs(1000)
//                .setMaxRetries(3)
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .setSerializationSchema(
//                        (input, context) -> new InsertOneModel<>(BsonDocument.parse(input)))
//                .build();

        DataStream<EditEvent> kafkaSource = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //{"id":1698671021,
        // "domain":"en.wikipedia.org",
        // "namespace":"main namespace",
        // "title":"List of Zimbabwe Twenty20 International cricket records",
        // "timestamp":"2023-11-27T19:19:29Z",
        // "user_name":"Faraz Master",
        // "user_type":"human",
        // "old_length":126239,
        // "new_length":126151}

        kafkaSource.filter(new FilterFunction<EditEvent>() {
                    @Override
                    public boolean filter(EditEvent editEvent) throws Exception {
                        if ("human".equalsIgnoreCase(editEvent.getUser_type())
                                && "main namespace".equalsIgnoreCase(editEvent.getNamespace())) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                })
                .keyBy(editEvent -> editEvent.getDomain())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new ProcessWindowFunction<EditEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<EditEvent, String, String, TimeWindow>.Context context, Iterable<EditEvent> iterable, Collector<String> collector) throws Exception {
                        long editSizeForWindow = 0L;
                        for (EditEvent editEvent : iterable) {
                            long newLength = editEvent.getNew_length();
                            long oldLength = editEvent.getOld_length();
                            long diff = newLength - oldLength;
                            editSizeForWindow += diff;
                        }
                        Map<String, Object> result = new HashMap<>();
                        result.put("domain", s);
                        result.put("totalEditSize", editSizeForWindow);
                        collector.collect(new ObjectMapper().writeValueAsString(result));
                    }
                }).print();


        // Execute program, beginning computation.
        env.execute("Flink Java API learning");
    }

}
