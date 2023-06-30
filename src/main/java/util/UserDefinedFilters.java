package util;

import data.GDELTEventData;
import events.MonitoringEvent;
import org.apache.flink.api.common.functions.FilterFunction;

public class UserDefinedFilters {

    /**
     * Filtering country Thailand: TH
     */
    public static class CountryFilter implements FilterFunction<GDELTEventData> {
        @Override
        public boolean filter(GDELTEventData in) throws Exception {
            if(in.getActionGeo_CountryCode().contentEquals("TH") )
                return true;
            else
                return false;
        }
    }

    public static class CountryAndEventFilter implements FilterFunction<GDELTEventData> {
        @Override
        public boolean filter(GDELTEventData in) throws Exception {
            if(in.getActionGeo_CountryCode().contentEquals("TH") &&
                    in.getEventRootCode().contentEquals("14"))
                return true;
            else
                return false;
        }
    }

    public static class RackFilter implements FilterFunction<MonitoringEvent> {
        int rackNoToFilter;
        public RackFilter(int num){
            this.rackNoToFilter = num;
        }
        @Override
        public boolean filter(MonitoringEvent event) throws Exception {
            if(event.getRackID() == rackNoToFilter)
                return true;
            else
                return false;
        }
    }


}
