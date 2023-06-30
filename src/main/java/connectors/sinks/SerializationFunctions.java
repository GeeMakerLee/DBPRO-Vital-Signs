package connectors.sinks;

import data.MimicWaveData;
import events.MonitoringEvent;
import events.TemperatureAlert;
import events.TemperatureWarning;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class SerializationFunctions {

    public static class Tuple3Serialization implements KeyedSerializationSchema<Tuple3<String,String,Double>> {
        @Override
        public byte[] serializeKey(Tuple3<String,String,Double> s) {
            return null;
        }
        @Override
        public byte[] serializeValue(Tuple3<String,String,Double> s) { return s.toString().getBytes(); }
        @Override
        public String getTargetTopic(Tuple3<String,String,Double> s) {
            return null;
        }
    }

    public static class MimicWaveDataSerialization implements KeyedSerializationSchema<MimicWaveData> {
        @Override
        public byte[] serializeKey(MimicWaveData s) {
            return null;
        }
        @Override
        public byte[] serializeValue(MimicWaveData s) {
            return s.toString().getBytes();
        }
        @Override
        public String getTargetTopic(MimicWaveData s) {
            return null;
        }
    }

    public static class MonitoringEventSerialization implements KeyedSerializationSchema<MonitoringEvent> {
        @Override
        public byte[] serializeKey(MonitoringEvent s) {
            return null;
        }
        @Override
        public byte[] serializeValue(MonitoringEvent s) {
            return s.toString().getBytes();
        }
        @Override
        public String getTargetTopic(MonitoringEvent s) {
            return null;
        }
    }

    public static class TemperatureWarningSerialization implements KeyedSerializationSchema<TemperatureWarning> {
        @Override
        public byte[] serializeKey(TemperatureWarning s) {
            return null;
        }
        @Override
        public byte[] serializeValue(TemperatureWarning s) {
            return s.toString().getBytes();
        }
        @Override
        public String getTargetTopic(TemperatureWarning s) {
            return null;
        }
    }

    public static class TemperatureAlertSerialization implements KeyedSerializationSchema<TemperatureAlert> {
        @Override
        public byte[] serializeKey(TemperatureAlert s) {
            return null;
        }
        @Override
        public byte[] serializeValue(TemperatureAlert s) {
            return s.toString().getBytes();
        }
        @Override
        public String getTargetTopic(TemperatureAlert s) {
            return null;
        }
    }
}
