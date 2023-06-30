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

package dfki.examples;

import connectors.sinks.SerializationFunctions;
import util.UserDefinedFunctions;
import util.UserDefinedFilters;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import events.MonitoringEvent;
import events.TemperatureAlert;
import events.TemperatureEvent;
import events.TemperatureWarning;
import connectors.sources.MonitoringEventSource;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * CEP example monitoring program
 * https://flink.apache.org/news/2016/04/06/cep-monitoring.html
 *
 * This example program generates a stream of monitoring events which are analyzed using Flink's CEP library.
 * The input event stream consists of temperature and power events from a set of racks. The goal is to detect
 * when a rack is about to overheat. In order to do that, we create a CEP pattern which generates a
 * TemperatureWarning whenever it sees two consecutive temperature events in a given time interval whose temperatures
 * are higher than a given threshold value. A warning itself is not critical but if we see two warning for the same rack
 * whose temperatures are rising, we want to generate an alert. This is achieved by defining another CEP pattern which
 * analyzes the stream of generated temperature warnings.
 *
 *  Apache Flink CEP description and examples:
 *  https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/libs/cep.html
 *
 *  For visualization:
 *    in kafka consola create a topic eventdata (if not created already)
 *       bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic eventdata
 *    start in visual:
 *      node kafka_consumer.js eventdata
 *    start from visual:
 *      firefox TemperatureCEPMonitoring.html
 */
public class TemperatureCEPMonitoring {
    private static final double TEMPERATURE_THRESHOLD = 80; //100;

    private static final int MAX_RACK_ID = 10;
    private static final long PAUSE = 500; //1000;   // 1 second = 1000
    private static final double TEMPERATURE_RATIO = 0.5;
    private static final double POWER_STD = 10;
    private static final double POWER_MEAN = 100;
    private static final double TEMP_STD = 20;
    private static final double TEMP_MEAN = 80;

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "eventdata";

        // Properties for writing stream in Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<MonitoringEvent> TempMonitoringKafkaProducer = new FlinkKafkaProducer<>(
                kafkaTopic,                                               // target topic
                new SerializationFunctions.MonitoringEventSerialization(),  // serialization schema
                properties,                                               // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);                // fault-tolerance


        FlinkKafkaProducer<TemperatureWarning> WarningMonitoringKafkaProducer = new FlinkKafkaProducer<>(
                kafkaTopic,                                               // target topic
                new SerializationFunctions.TemperatureWarningSerialization(),  // serialization schema
                properties,                                               // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);                // fault-tolerance

        FlinkKafkaProducer<TemperatureAlert> AlertMonitoringKafkaProducer = new FlinkKafkaProducer<>(
                kafkaTopic,                                               // target topic
                new SerializationFunctions.TemperatureAlertSerialization(),  // serialization schema
                properties,                                               // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);                // fault-tolerance


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Input stream of monitoring events, created by the source
        DataStream<MonitoringEvent> inputEventStreamAll = env
                .addSource(new MonitoringEventSource(
                        MAX_RACK_ID,
                        PAUSE,
                        TEMPERATURE_RATIO,
                        POWER_STD,
                        POWER_MEAN,
                        TEMP_STD,
                        TEMP_MEAN))
                .assignTimestampsAndWatermarks(new UserDefinedFunctions.ExtractTimestampEvent());

        //inputEventStreamAll.print();

        // Filter rack 1 for simple visualization
        DataStream<MonitoringEvent> inputEventStream = inputEventStreamAll.filter(new UserDefinedFilters.RackFilter(1));


        inputEventStream.print();
        // Sink in kafka for visualization
        inputEventStream.addSink(TempMonitoringKafkaProducer);

        // Warning pattern: Two consecutive temperature events whose temperature is higher than the given threshold
        // appearing within a time interval of 10 seconds
        Pattern<MonitoringEvent, ?> warningPattern = Pattern.<MonitoringEvent>begin("first")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    private static final long serialVersionUID = -6301755149429716724L;

                    @Override
                    public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                         return value.getTemperature() >= TEMPERATURE_THRESHOLD;
                    }
                })
                .next("second")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    private static final long serialVersionUID = 2392863109523984059L;

                    @Override
                    public boolean filter(TemperatureEvent value, Context<TemperatureEvent> ctx) throws Exception {
                        return value.getTemperature() >= TEMPERATURE_THRESHOLD;
                    }
                })
                .within(Time.seconds(10));

        // Create a pattern stream from our warning pattern
        PatternStream<MonitoringEvent> tempPatternStream = CEP.pattern(inputEventStream.keyBy("rackID"),
                warningPattern);

        // Generate temperature warnings for each matched warning pattern
        DataStream<TemperatureWarning> warnings = tempPatternStream.select(new GenerateTemperatureWarning());

        // Alert pattern: Two consecutive temperature warnings appearing within a time interval of 20 seconds
        Pattern<TemperatureWarning, ?> alertPattern = Pattern.<TemperatureWarning>begin("first")
                .next("second")
                .within(Time.seconds(20));

        // Create a pattern stream from our alert pattern
        PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(
                warnings.keyBy("rackID"),
                alertPattern);

        // Generate a temperature alert only iff the second temperature warning's average temperature is higher than
        // first warning's temperature
        DataStream<TemperatureAlert> alerts = alertPatternStream.flatSelect(new GenerateTemperatureAlert());

        // Print the warning and alert events to stdout
        warnings.print();
        // Sink in kafka for visualization
        warnings.addSink(WarningMonitoringKafkaProducer);

        alerts.print();
        // Sink in kafka for visualization
        alerts.addSink(AlertMonitoringKafkaProducer);


        env.execute("CEP monitoring job");
    }



    private static class GenerateTemperatureWarning implements PatternSelectFunction<MonitoringEvent, TemperatureWarning> {

        @Override
        public TemperatureWarning select(Map<String, List<MonitoringEvent>> pattern) throws Exception {

            TemperatureEvent first = (TemperatureEvent) pattern.get("first").get(0);
            TemperatureEvent second = (TemperatureEvent) pattern.get("second").get(0);
            System.out.println("  WARN:  first:" + first.toString() + "  second: " + second.toString()  +  "  list pattern size:" + pattern.size());
            // returning the average of the two temperatures:
            return new TemperatureWarning(second.getTimeStampMs(), first.getRackID(), (first.getTemperature() + second.getTemperature()) / 2);

            // returning the second temperature:
            //return new TemperatureWarning(second.getTimeStampMs(), first.getRackID(), second.getTemperature());
        }
    }


    private static class GenerateTemperatureAlert implements PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert> {

        @Override
        public void flatSelect(Map<String, List<TemperatureWarning>> pattern, Collector<TemperatureAlert> out) throws Exception {
            TemperatureWarning first = pattern.get("first").get(0);
            TemperatureWarning second = pattern.get("second").get(0);
            if (first.getAverageTemperature() < second.getAverageTemperature()) {

                System.out.println("  ALERT:  first:" + first.toString() + "  second: " + second.toString() );

                // if returning the average of the two consecutive values:
                //out.collect(new TemperatureAlert(second.getTimeStampMs(), second.getRackID(), (first.getAverageTemperature() + second.getAverageTemperature()) / 2));

                // returning the second value:
                out.collect(new TemperatureAlert(second.getTimeStampMs(), second.getRackID(), second.getAverageTemperature()));
            }
        }
    }



}
