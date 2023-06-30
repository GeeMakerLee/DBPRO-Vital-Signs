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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.Thread.sleep;

/**
 * @author Marcela Charfuelan
 *
 * In this example a time series data is processed in streaming mode.
 * this class includes:
 *   - loading the data as a stream series, using event time
 *   - applying a tumbling and a sliding window with various time settings
 *   - visualization (optional)
 *
 * Run with:
 *   --input ./src/main/resources/dataDWD-station-ID-433.csv
 *
 * The data used in this example is provided by Deutscher Wetterdienst (DWD)
 * More information can be found here:
 *     https://www.dwd.de/DE/klimaumwelt/cdc/cdc_node.html
 *     ftp://opendata.dwd.de/climate_environment/CDC/Readme_intro_CDC_ftp.pdf
 * The data set used here was extracted with the R package:
 *     https://bookdown.org/brry/rdwd/
 */
public class StreamingJob {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		String inputFileName = params.get("input");

		// 1. Obtain an execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.setParallelism(1);

		// 2. Load/create the initial data
		// Load the data in a DataStream
		//         Tuple3<key,measurement,date>
		DataStream<Tuple3<String, Double, String>> dailyTemperatureData = env.readTextFile(inputFileName)
				                                                .map(new ParseData())
				                                                .assignTimestampsAndWatermarks(new ExtractTimestamp());
		// The input data can be print on consola
		dailyTemperatureData.print();

		// 3. Specify transformations on this data
        // Process the input stream, applying some operator or UDF
		dailyTemperatureData
				.keyBy(0)
				//.timeWindow(Time.hours(24))
				//       Tumbling window
				.window(TumblingEventTimeWindows.of(Time.days(30)))
				//       Sliding window:
				//.window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
				//       Average the window
				.apply(new MovingAverageFunction())

				//4. Specify where to put the results of your computations,
				// print the output averaged data on consola
		        .print();
				// or save the averaged data in a file
				//.writeAsCsv("/tmp/dwd_ma_temperature.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // 5. Trigger the program execution
		env.execute("MATemperatureExample");
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

			//System.out.println(winKey + " winsum=" +  winsum + "  count=" + count + "  avg=" + avg + "  date=" + sdf.format(date));

			Tuple3<String, Double, String> windowAvg = new Tuple3<>(winKey,avg,sdf.format(date));

			out.collect(windowAvg);

		}
	}


	/**
	 * Receive as input one line from the input file
	 * Returns:
	 *    Tuple3<Key, temperature messurement, date >
	 */
	private static class ParseData extends RichMapFunction<String, Tuple3<String, Double, String>> {
		private static final long serialVersionUID = 1L;


		@Override
		public Tuple3<String, Double, String> map(String record) {
			//String rawData = record.substring(1, record.length() - 1);
			String rawData = record;
			String[] data = rawData.split(",");

			// the data look like this...
			//MESS_DATUM,SDK.Sonnenscheindauer,TMK.Lufttemperatur,TXK.Lufttemperatur_Max,TNK.Lufttemperatur_Min
			// 2019-05-06,5.967,8.5,13.4,3.5
			// 2019-05-07,6.467,8.8,13.5,4.3
			// 2019-05-08,9.1,12.2,18.3,2.8
			// 2019-05-09,2.783,13.9,18.4,10.4
			// 2019-05-10,2.533,13.2,16.8,8.5
			//
			// to introduce a delay on the stream
/*
			try {
				sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
*/
			Tuple3<String, Double, String> dwdData = new Tuple3<String, Double, String>("Lufttemperatur", Double.valueOf(data[2]), data[0]);
			return(dwdData);

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

}
