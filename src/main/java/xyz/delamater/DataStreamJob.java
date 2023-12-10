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

import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class DataStreamJob {

    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void runPipeline(StreamExecutionEnvironment env, KafkaSource<EditEvent> source, MongoSink<String> sink) throws Exception {

        //{"id":1698671021,
        // "domain":"en.wikipedia.org",
        // "namespace":"main namespace",
        // "title":"List of Zimbabwe Twenty20 International cricket records",
        // "timestamp":"2023-11-27T19:19:29Z",
        // "user_name":"Faraz Master",
        // "user_type":"human",
        // "old_length":126239,
        // "new_length":126151}

        DataStream<EditEvent> kafkaSource = env
                .fromSource(source,
                        WatermarkStrategy.<EditEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(30), Duration.ofSeconds(10)),
                        "Kafka Source"
                );

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
                }).map(new MapFunction<EditEvent, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(EditEvent editEvent) throws Exception {
                        return new Tuple3<>(editEvent.getDomain(), editEvent.getNew_length(), editEvent.getOld_length());
                    }
                })
                .keyBy(r -> r.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .apply(new MyWindowFunction())
                .sinkTo(sink);

        // Execute program, beginning computation.
        env.execute("Flink Java API learning");
    }

    static class MyWindowFunction implements WindowFunction<Tuple3<String, Long, Long>, String, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Tuple3<String, Long, Long>> iterable, Collector<String> collector) throws Exception {
            long total = 0L;
            for (Tuple3<String, Long, Long> tuple : iterable) {
                total += Math.abs(tuple.f1 - tuple.f2);
            }
            Document doc = new Document();
            doc.put("domain", key);
            doc.put("edit_size", total);
            doc.put("start", timeWindow.getStart());
            doc.put("end", timeWindow.getEnd());
            collector.collect(doc.toJson());
        }
    }

    public static void main(String[] args) {
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

        try {
            runPipeline(env, source, sink);
        } catch (Exception e) {
            LOG.error("Exception in flink pipeline", e);
            e.printStackTrace();
        }
    }

}
