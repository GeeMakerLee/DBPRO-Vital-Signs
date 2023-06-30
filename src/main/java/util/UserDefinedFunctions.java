package util;

import data.GDELTEventData;
import data.MimicWaveData;
import events.MonitoringEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UserDefinedFunctions {

    /**
     * This function extracts the time stamp from a mimic event
     */
    public static class ExtractTimestampMimic extends AscendingTimestampExtractor<MimicWaveData> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(MimicWaveData element) {
            return element.getTimeStampMs();
        }
    }

    /**
     * This function extracts the time stamp from a GDELT event
     */
    public static class ExtractTimestampGDELTEvent extends AscendingTimestampExtractor<GDELTEventData> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(GDELTEventData element) {
            return element.getTimeStampMs();
        }
    }

    /**
     * This function extracts the time stamp from a MonitoringEvent
     */
    public static class ExtractTimestampEvent extends AscendingTimestampExtractor<MonitoringEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(MonitoringEvent element) {
            return element.getTimeStampMs();
        }
    }

    /**
     * This function select two fields from Gdelt as keys
     */
    public static class CountryAndEventCodeKeySelector implements KeySelector<GDELTEventData, Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> getKey(GDELTEventData value) throws Exception {
            return Tuple2.of(value.getActionGeo_CountryCode(), value.getEventRootCode());
        }
    }


    /**
     * This function is used to parse line by line extracted with GDELTZipDataSourceFuntion()
     */
    public static class ParseLineGdeltEventData implements FlatMapFunction<String, GDELTEventData> {
        @Override
        public void flatMap(String line, Collector<GDELTEventData> collector) throws ParseException {

            String rawData = line;
            String[] data = rawData.split("\\t");
            //System.out.println("  FILE:" + path.toString() + "  No. FIELDS: " + data.length + "  LINE:" + line);
            // TODO: confirm in Event data lines can have 57 or 58 fields... why? depends on the year?
            if( (data.length == 57) || (data.length == 58) ) {
                String eventRecordId = data[0];
                Date date = new SimpleDateFormat("yyyyMMdd").parse(data[1]);
                String monthYear = data[2];
                String full_date = data[1];
                String EventBaseCode;

                if (data[27].trim().isEmpty() || data[27] == "" || data[27] == "---"){
                    EventBaseCode = "";
                } else {
                    EventBaseCode = data[27];
                }
                String EventRootCode = data[28];
                int NumMentions = Integer.parseInt(data[31]);
                float AvgTone = Float.parseFloat(data[34]);
                String ActionGeo_Fullname = data[50];
                String ActionGeo_CountryCode =data[51];
                float GoldsteinScale;
                if (data[30].isEmpty()){
                    GoldsteinScale = 0;
                } else {
                    GoldsteinScale = Float.parseFloat(data[30]);
                }
                String ActionGeo_Lat,ActionGeo_Long;
                if (data[53].isEmpty()){
                    ActionGeo_Lat = "";
                }else {
                    ActionGeo_Lat = data[53];
                }
                if (data[54].isEmpty()){
                    ActionGeo_Long = "";
                } else {
                    ActionGeo_Long = data[54];
                }

                collector.collect(new GDELTEventData("gdelt_event", eventRecordId, date,full_date, monthYear, EventBaseCode,
                        EventRootCode, GoldsteinScale, NumMentions, AvgTone,ActionGeo_Fullname, ActionGeo_CountryCode, ActionGeo_Lat,
                        ActionGeo_Long));
            } else {
                System.out.println("  LINE PROBLEM:  No. FIELDS: " + data.length + "  LINE:" + line);
            }
        }
    }

}
