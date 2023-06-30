package data;

import java.util.Date;

/**
 * check rules for POJO types:
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html#flinks-typeinformation-class
 */
public class GDELTEventData {

    public String key;

  /*  "GlobalEventID","Day","MonthYear","Year","FractionDate","Actor1Code","Actor1Name","Actor1CountryCode",
            "Actor1KnownGroupCode","Actor1EthnicCode","Actor1Religion1Code","Actor1Religion2Code","Actor1Type1Code",
            "Actor1Type2Code","Actor1Type3Code","Actor2Code","Actor2Name","Actor2CountryCode",
            "Actor2KnownGroupCode","Actor2EthnicCode","Actor2Religion1Code","Actor2Religion2Code","Actor2Type1Code",
            "Actor2Type2Code","Actor2Type3Code","IsRootEvent","EventCode","EventBaseCode","EventRootCode","QuadClass",
            "GoldsteinScale","NumMentions","NumSources","NumArticles","AvgTone","Actor1Geo_Type","Actor1Geo_Fullname",
            "Actor1Geo_CountryCode","Actor1Geo_ADM1Code","Actor1Geo_Lat","Actor1Geo_Long","Actor1Geo_FeatureID",
            "Actor2Geo_Type","Actor2Geo_Fullname","Actor2Geo_CountryCode","Actor2Geo_ADM1Code","Actor2Geo_Lat",
            "Actor2Geo_Long","Actor2Geo_FeatureID","ActionGeo_Type","ActionGeo_Fullname","ActionGeo_CountryCode",
            "ActionGeo_ADM1Code","ActionGeo_Lat","ActionGeo_Long","ActionGeo_FeatureID","DATEADDED","SOURCEURL"*/

    private String GlobalEventID;
    // TODO: adapt the right variable types
    private Date date;
    private String full_date;
    private String MonthYear;
    private String EventBaseCode;
    private String EventRootCode;
    private float GoldsteinScale;
    private int NumMentions;
    private float AvgTone;
    private String ActionGeo_Fullname;
    private String ActionGeo_CountryCode;
    private String ActionGeo_Lat;
    private String ActionGeo_Long;

    public GDELTEventData(){}

    public GDELTEventData(
            String key,
            String GlobalEventID,
            Date date,
            String full_date,
            String MonthYear,
            String EventBaseCode,
            String EventRootCode,
            float GoldsteinScale,
            int NumMentions,
            float AvgTone,
            String ActionGeo_Fullname,
            String ActionGeo_CountryCode,
            String ActionGeo_Lat,
            String ActionGeo_Long) {
        this.key = key;
        this.GlobalEventID = GlobalEventID;
        // TODO: adapt the right variable types
        this.date = date;
        this.full_date = full_date;
        this.MonthYear = MonthYear;
        this.EventBaseCode = EventBaseCode;
        this.EventRootCode = EventRootCode;
        this.GoldsteinScale = GoldsteinScale;
        this.NumMentions = NumMentions;
        this.AvgTone = AvgTone;
        this.ActionGeo_Fullname = ActionGeo_Fullname;
        this.ActionGeo_CountryCode = ActionGeo_CountryCode;
        this.ActionGeo_Lat = ActionGeo_Lat;
        this.ActionGeo_Long = ActionGeo_Long;
    }

    public Date getDate() {
        return date;
    }
    public long getTimeStampMs() { return date.getTime(); }

    public String getKey() {
        return key;
    }
    public void setKey(String val) { this.key = val; }

    public String getEventBaseCode() {
        return EventBaseCode;
    }
    public void setEventBaseCode(String eventBaseCode) {
        EventBaseCode = eventBaseCode;
    }

    public String getEventRootCode() {return EventRootCode; }
    public void setEventRootCode(String eventRootCode) {
        EventRootCode = eventRootCode;
    }

    public float getGoldsteinScale() {
        return GoldsteinScale;
    }
    public void setGoldsteinScale(float goldsteinScale) {
        GoldsteinScale = goldsteinScale;
    }

    public int getNumMentions() {
        return NumMentions;
    }
    public void setNumMentions(int numMentions) {
        NumMentions = numMentions;
    }

    public float getAvgTone() {
        return AvgTone;
    }
    public void setAvgTone(float avgTone) {
        AvgTone = avgTone;
    }

    public String getActionGeo_CountryCode() {
        return ActionGeo_CountryCode;
    }
    public void setActionGeo_CountryCode(String actionGeo_CountryCode) { ActionGeo_CountryCode = actionGeo_CountryCode; }

    public String getActionGeo_Lat() {
        return ActionGeo_Lat;
    }
    public void setActionGeo_Lat(String actionGeo_Lat) {
        ActionGeo_Lat = actionGeo_Lat;
    }

    public String getActionGeo_Long() {
        return ActionGeo_Long;
    }
    public void setActionGeo_Long(String actionGeo_Long) {
        ActionGeo_Long = actionGeo_Long;
    }

    public String getActionGeo_Fullname() {
        return ActionGeo_Fullname;
    }
    public void setActionGeo_Fullname(String actionGeo_Fullname) {
        ActionGeo_Fullname = actionGeo_Fullname;
    }

    public String getFull_date() {
        return full_date;
    }
    public void setFull_date(String full_date) {
        this.full_date = full_date;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        //sb.append(GlobalEventID).append(",");
        //sb.append(date).append(",");
        sb.append(full_date).append(",");
        //sb.append(MonthYear).append(",");
        //sb.append(EventBaseCode).append(",");
        sb.append(EventRootCode).append(",");
        //sb.append(GoldsteinScale).append(",");
        sb.append(NumMentions).append(",");
        sb.append(AvgTone).append(",");
        sb.append(ActionGeo_CountryCode).append(",");
        //sb.append(ActionGeo_Lat).append(",");
        //sb.append(ActionGeo_Long);
        return sb.toString();
    }
}
