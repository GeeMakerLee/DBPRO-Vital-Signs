package dfki.examples;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import connectors.sources.DwdDataSourceFunction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Run with:
 *    --input ./src/main/resources/dataDWD-station-ID-433.csv
 *    For visualization:
 *    in kafka consola create a topic dwddata (if not created already)
 *       bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dwddata
 *    start in visual:
 *      node kafka_consumer.js dwddata
 *    start from visual:
 *      firefox StreamingVisualizationJob.html
 */
public class StreamingVisualizationJob {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String inputFile = params.get("input");

        // Properties for writing stream in Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // Create a Kafka producer to sink the stream into a kafka topic
        FlinkKafkaProducer<Tuple3<String, Double, String>> dwdTempDataKafkaProducer = new FlinkKafkaProducer<>(
                "dwddata",                     // target topic
                new Tuple3Serialization(),                  // serialization schema
                properties,                                 // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);  // fault-tolerance


        // 1. Obtain an execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);
        @SuppressWarnings({"rawtypes", "serial"})

        // 2. Load/create the initial data
        // Load the data in a DataStream
        // NEW: here we use a source function to load the data from a file and create a DataStream
        DataStream<Tuple3<String, Double, String>> dailyTemperatureData = env
                         .addSource(new DwdDataSourceFunction(inputFile))
                         .assignTimestampsAndWatermarks(new ExtractTimestamp());

        // output 2.1
        // The data can be print on consola
        dailyTemperatureData.print();
        // output 2.2
        //  The data can be sink in Kafka for visualization
     //   dailyTemperatureData.addSink(dwdTempDataKafkaProducer);

        // 3. Specify transformations on this data
        // Process the input stream, applying some operator or UDF
        DataStream<Tuple3<String, Double, String>> dailyTemperatureDataMA = dailyTemperatureData
                .keyBy(0)
                //.timeWindow(Time.hours(24))
                //       Tumbling window
                //.window(TumblingEventTimeWindows.of(Time.days(3)))
                //       Sliding window:
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
                //       Average the window
                .apply(new MovingAverageFunction());

        dailyTemperatureDataMA
                //4. Specify where to put the results of your computations,
                // print the output averaged data on consola
                // output 4.1
                // print the averaged data on consola
                // .print();
                // output 4.2
                // sink in kafka for visualization
                .addSink(dwdTempDataKafkaProducer);
                // output 4.3
                // save the average data for every key in a different series
                //.name("waterLevelAvg");
                //.writeAsCsv("/tmp/noaa_water_level_averaged.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("AvgTemperatureExample");
    }


    /**
     * This function receives a "window" of data, it calculates the average of the values in the window
     * Returns a tuple of three elements:
     *    Tuple3<key of the window, average, end date of the window>
     */
    private static class MovingAverageFunction implements WindowFunction<Tuple3<String, Double, String>, Tuple3<String, Double, String>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple arg0, TimeWindow window, Iterable<Tuple3<String, Double, String>> input, Collector<Tuple3<String, Double, String>> out) {
            int count = 0;
            double winsum = 0;
            String winKey = input.iterator().next().f0; // get the key of this window  "movingAvg";
            winKey = "MA"+winKey;
            // get the sum of the elements in the window
            for (Tuple3<String, Double, String> in: input) {
                winsum = winsum + in.f1;
                count++;
            }
            Double avg = winsum/(1.0 * count);

            // to convert time stamp in epoch to date
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date(window.getEnd());

            System.out.println(winKey + " winsum=" +  winsum + "  count=" + count + "  avg=" + avg + "  date=" + sdf.format(date));

            Tuple3<String, Double, String> windowAvg = new Tuple3<>(winKey,avg,sdf.format(date));

            out.collect(windowAvg);

        }
    }


    /**
     * Extract from each element of the stream the date in epoch format (time stamp in miliseconds)
     */
    private static class ExtractTimestamp extends AscendingTimestampExtractor<Tuple3<String, Double, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple3<String, Double, String> element) {
            // to convert string date into time stamp in epoch
            Date date = null;
            try {
                date = new SimpleDateFormat("yyyy-MM-dd").parse(element.f2);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return date.getTime();  // returns time stamp in milliseconds
        }
    }

    /**
     * Serialise data befre sending to Kafka
     */
    public static class Tuple3Serialization implements KeyedSerializationSchema<Tuple3<String, Double, String>> {
        @Override
        public byte[] serializeKey(Tuple3<String, Double, String> s) {
            return null;
        }
        @Override
        public byte[] serializeValue(Tuple3<String, Double, String> s) {return s.toString().getBytes(); }
        @Override
        public String getTargetTopic(Tuple3<String, Double, String> s) {
            return null;
        }
    }

}
