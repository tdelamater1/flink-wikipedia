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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
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

        String mongoUser = System.getenv("MONGO_USER");
        String mongoPass = System.getenv("MONGO_PASS");

        MongoSink<String> sink = MongoSink.<String>builder()
                .setUri("mongodb://" + mongoUser + ":" + mongoPass + "@192.168.4.100:27017/wikipedia")
                .setDatabase("wikipedia")
                .setCollection("edits")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema(
                        (input, context) -> new InsertOneModel<>(BsonDocument.parse(input)))
                .build();

        DataStream<EditEvent> kafkaSource = env
                .fromSource(source, getWatermarkStrategy(), "Kafka Source");

        //{"id":1698671021,
        // "domain":"en.wikipedia.org",
        // "namespace":"main namespace",
        // "title":"List of Zimbabwe Twenty20 International cricket records",
        // "timestamp":"2023-11-27T19:19:29Z",
        // "user_name":"Faraz Master",
        // "user_type":"human",
        // "old_length":126239,
        // "new_length":126151}

        
        KeyedStream<EditEvent, String> filteredAndKeyed = kafkaSource.filter(new FilterFunction<EditEvent>() {
            @Override
            public boolean filter(EditEvent editEvent) throws Exception {
                if ("en.wikipedia.org".equals(editEvent.getDomain())
                        && "human".equals(editEvent.getUser_type())
                        && "main namespace".equals(editEvent.getNamespace())) {
                    return true;
                } else {
                    return false;
                }
            }
        }).keyBy(EditEvent::getUser_name);

        WindowedStream<EditEvent, String, TimeWindow> filterKeyedAndWindowed = filteredAndKeyed.window(TumblingEventTimeWindows.of(Time.minutes(60L)));

        DataStream<Tuple2<String, Long>> totalEditSizeByTitle = filterKeyedAndWindowed.process(new ProcessWindowFunction<EditEvent, Tuple2<String, Long>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<EditEvent> elements, Collector<Tuple2<String, Long>> out) {
                long totalEditSize = 0L;
                LOG.info("Current watermark: {}", context.currentWatermark());
                LOG.info("Processing window for key: {}", key);
                for (EditEvent element : elements) {
                    LOG.info("Processing event with timestamp: {}", element.getTimestamp());
                    long oldLength = element.getOld_length();
                    long newLength = element.getNew_length();
                    long editSize = newLength - oldLength;
                    totalEditSize += editSize;
                }
                out.collect(new Tuple2<>(key, totalEditSize));
            }
        });


        DataStream<String> result = totalEditSizeByTitle.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws JsonProcessingException {
                Map<String, Object> result = new HashMap<>();
                result.put("title", value.f0);
                result.put("totalEditSize", value.f1);
                LOG.info("Result: {}", result);
                return new ObjectMapper().writeValueAsString(result);
            }
        });

        result.print();

//        result.sinkTo(sink);

        // Execute program, beginning computation.
        env.execute("Flink Java API learning");
    }

    private static WatermarkStrategy<EditEvent> getWatermarkStrategy() {
        return WatermarkStrategy.<EditEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30L))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<EditEvent>() {
                            @Override
                            public long extractTimestamp(EditEvent editEvent, long l) {
                                try {
                                    //"timestamp":"2023-11-27T19:19:29Z"
                                    //"timestamp": "2023-12-05T00:51:21Z"
                                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                                    format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
                                    return format.parse(editEvent.getTimestamp()).getTime();
                                    //new Date();
                                    //LOG.info("Timestamp: {} Now: {}", time, new Date().getTime());
                                } catch (ParseException e) {
                                    throw new RuntimeException("Error parsing data for watermark. ", e);
                                }
                            }
                        }
                );
    }
}
