package util;

import data.GDELTEventData;
import data.MimicWaveData;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.Thread.sleep;

public class UserDefinedWindowFunctions {

    /**
     * For more details on moving average see:
     *    https://otexts.com/fpp2/moving-averages.html
     *    here we apply a moving average to these fields of MimicWaveData
     *             Double HR,
     *             Double ABPMean,
     *             Double PAPMean,
     *             Double CVP,
     *             Double Pulse,
     *             Double Resp,
     *             Double SpO2,
     *             Double NBPMean,
     */
    public static class MovingAverageFunction extends ProcessWindowFunction<MimicWaveData, MimicWaveData, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<MimicWaveData> iterable, Collector<MimicWaveData> collector) throws Exception {
        //public static class MovingAverageFunction implements WindowFunction<MimicWaveData, MimicWaveData, Tuple, TimeWindow> {
        //@Override
        //public void apply(Tuple arg0, TimeWindow window, Iterable<MimicWaveData> input, Collector<MimicWaveData> out) {
            // 'HR','ABPSys','ABPDias','ABPMean','PAPSys','PAPDias','PAPMean','CVP','PULSE','RESP','SpO2','NBPSys','NBPDias','NBPMean'
            //double winsum[] = new double[8];
            double winsum[] = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
            int count = 0;
            // get the sum of the elements in the window
            for (MimicWaveData in: iterable) {
                //System.out.println("  WIN: " + in.toString());
                winsum[0] = winsum[0] + in.getHR();
                winsum[1] = winsum[1] + in.getABPSys();
                winsum[2] = winsum[2] + in.getABPDias();
                winsum[3] = winsum[3] + in.getABPMean();
                winsum[4] = winsum[4] + in.getPAPSys();
                winsum[5] = winsum[5] + in.getPAPDias();
                winsum[6] = winsum[6] + in.getPAPMean();
                winsum[7] = winsum[7] + in.getCVP();
                winsum[8] = winsum[8] + in.getPulse();
                winsum[9] = winsum[9] + in.getResp();
                winsum[10] = winsum[10] + in.getSpO2();
                winsum[11] = winsum[11] + in.getNBPSys();
                winsum[12] = winsum[12] + in.getNBPDias();
                winsum[13] = winsum[13] + in.getNBPMean();
                count++;
            }

            String winKey = iterable.iterator().next().getKey();
            String winrecordId = iterable.iterator().next().getRecordId();
            winsum[0] = winsum[0]/(1.0 * count);
            winsum[1] = winsum[1]/(1.0 * count);
            winsum[2] = winsum[2]/(1.0 * count);
            winsum[3] = winsum[3]/(1.0 * count);
            winsum[4] = winsum[4]/(1.0 * count);
            winsum[5] = winsum[5]/(1.0 * count);
            winsum[6] = winsum[6]/(1.0 * count);
            winsum[7] = winsum[7]/(1.0 * count);
            winsum[8] = winsum[8]/(1.0 * count);
            winsum[9] = winsum[9]/(1.0 * count);
            winsum[10] = winsum[10]/(1.0 * count);
            winsum[11] = winsum[11]/(1.0 * count);
            winsum[12] = winsum[12]/(1.0 * count);
            winsum[13] = winsum[13]/(1.0 * count);

            MimicWaveData event = new MimicWaveData(winKey, winrecordId,
                    winsum[0],
                    winsum[1],
                    winsum[2],
                    winsum[3],
                    winsum[4],
                    winsum[5],
                    winsum[6],
                    winsum[7],
                    winsum[8],
                    winsum[9],
                    winsum[10],
                    winsum[11],
                    winsum[12],
                    winsum[13],
                    context.window().getEnd());

            // introduce a delay to make easier the visualization
            try {
                 sleep(1000);
            } catch (InterruptedException e) {
                 e.printStackTrace();
            }

            collector.collect(event);

        }
    }


    /**
     * Aggregate number of mentions per window
     * input:
     *   GDELTEventData
     * returns:
     *   Tuple4<date, country-key, event-code-key, num-mentions per window>
     */
    public static class AggregateEventsPerCountryPerDay extends ProcessWindowFunction<GDELTEventData, Tuple4<String, String, String, Integer>, Tuple2<String, String>, TimeWindow> {
        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<GDELTEventData> iterable, Collector<Tuple4<String, String, String, Integer>> collector) throws Exception {
            int mentions = 0;

            for (GDELTEventData in: iterable) {
                mentions = mentions + in.getNumMentions();
                //System.out.println("    LINE: " + key + "  " + in.getTimeStampMs() + "  " + in.getFull_date() + "  " + date + "  " + in.getEventRootCode() + "  " + in.getNumMentions() + "  " + in.getAvgTone() + "  " + count + " " + " " + in.getActionGeo_Long() + " " + in.getActionGeo_Lat());
            }
            // a window has: context.window().getStart() and context.window().getEnd()
            // here it is return the end time of the window
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date end = new Date(context.window().getEnd());

            //Tuple4<date, country-key, event-code-key, num-mentions per window>
            Tuple4<String, String, String, Integer> aggregatedEvent =
                    new Tuple4<String, String, String, Integer>(sdf.format(end), key.f0, key.f1, mentions);
            collector.collect(aggregatedEvent);
        }
    }


}
