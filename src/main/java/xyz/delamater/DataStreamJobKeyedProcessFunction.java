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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;

public class DataStreamJobKeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "192.168.4.130:9092";
        KafkaSource<EditEvent> source = KafkaSource.<EditEvent>builder().
                setBootstrapServers(brokers)
                .setTopics("wikipedia-events")
                .setGroupId("consumer-group-1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        String mongoUser = System.getenv("MONGO_USER");
        String mongoPass = System.getenv("MONGO_PASS");

        System.out.println("mongoUser: " + mongoUser);
        System.out.println("mongoPass: " + mongoPass);


//        MongoSink<Document> sink = MongoSink.<Document>builder()
//                .setUri("mongodb://" + mongoUser + ":" + mongoPass + "@192.168.4.100:27017/wikipedia")
//                .setDatabase("wikipedia")
//                .setCollection("edits")
//                .setBatchSize(1000)
//                .setBatchIntervalMs(1000)
//                .setMaxRetries(3)
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .setSerializationSchema(
//                        (input, context) -> new InsertOneModel<>(BsonDocument.parse(input.toJson())))
//                .build();

        DataStreamSource<EditEvent> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

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

        KeyedStream<EditEvent, String> humanEditsByTitle = kafkaSource.filter(new FilterFunction<EditEvent>() {
            @Override
            public boolean filter(EditEvent editEvent) {
                if (editEvent.getDomain().equals("en.wikipedia.org")
                        && editEvent.getUser_type().equals("human")
                        && editEvent.getNamespace().equals("main namespace")) {
                    return true;
                } else {
                    return false;
                }
            }
        }).keyBy(EditEvent::getTitle);

        // determine the size of the edit (new_length - old_length) and keep a running total
        humanEditsByTitle.process(new KeyedProcessFunction<String, EditEvent, Document>() {

            ValueState<Long> totalEditSizeState;

            // we initialize the state in lifecycle method
            @Override
            public void open(Configuration parameters) throws Exception {
                totalEditSizeState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("totalEditSize", Long.class));
            }

            @Override
            public void processElement(EditEvent editEvent, KeyedProcessFunction<String, EditEvent, Document>.Context context, Collector<Document> collector) throws Exception {
                Long totalEditSize = totalEditSizeState.value();
                if (totalEditSize == null) {
                    totalEditSize = 0L;
                }
                long oldLength = editEvent.getOld_length();
                long newLength = editEvent.getNew_length();
                long editSize = newLength - oldLength;
                totalEditSize += editSize;
                Document doc = new Document();
                doc.put("title", editEvent.getTitle());
                doc.put("edit_size", totalEditSize);
                totalEditSizeState.update(totalEditSize);
                collector.collect(doc);
            }
        }).print();



//
//        humanEdits.sinkTo(sink);
//
//
//        // map the json node to a tuple of (user_name, title, 1)
//        humanEdits.map(new Tokenizer())
//                .keyBy(0, 1)
//                .sum(2).print();


        // Execute program, beginning computation.
        env.execute("Flink Java API learning");
    }

    public static final class Tokenizer implements MapFunction<EditEvent, Tuple3<String, String, Integer>> {
        public Tuple3<String, String, Integer> map(EditEvent editEvent) {
            return new Tuple3<>(editEvent.getUser_name(), editEvent.getTitle(), 1);
        }
    }
}
