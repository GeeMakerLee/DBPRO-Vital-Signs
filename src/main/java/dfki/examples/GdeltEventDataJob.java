package dfki.examples;

import connectors.sources.GDELTZipDataSourceFuntion;
import data.GDELTEventData;
import util.UserDefinedFilters.*;
import util.UserDefinedFunctions.*;
import util.UserDefinedWindowFunctions.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * Parameters to run (example):
 * --input ./src/main/resources/2006/ --output event-out.csv
 */
public class GdeltEventDataJob {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String gdeltFilesDirectory = parameters.get("input");
        String outputCsvFile = parameters.get("output");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Reading line by line raw data from the Event data file
        DataStream<String> gdeltRawEventData = env.addSource(new GDELTZipDataSourceFuntion(gdeltFilesDirectory));

        // Once we read the lines of every raw file, parse line by line to extract one GDELT EventData per line
        DataStream<GDELTEventData> gdeltEventData = gdeltRawEventData
                .flatMap(new ParseLineGdeltEventData())
                .assignTimestampsAndWatermarks(new ExtractTimestampGDELTEvent());

        // Processing GDELT Events
        gdeltEventData
                // In this example we are filtering Thailand country
                .filter(new CountryFilter())
                // KeyBy country and event code
                .keyBy(new CountryAndEventCodeKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(365)))
                // Aggregrating a period of time (window) per country and per Event code:
                // Tuple4<date, country-key, event-code-key, num-mentions per window>
                .process(new AggregateEventsPerCountryPerDay())

                // print on consola
                .print();

                // write result in a file
                //.writeAsCsv(outputCsvFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        env.execute("GdeltEventJob");

    }


}
