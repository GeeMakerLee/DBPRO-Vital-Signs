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

package events;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TemperatureAlert {
    private long timeStampMs;
    private int rackID;
    private double averageTemperature;

    public TemperatureAlert(long ts, int rackID, double averageTemperature) {
        this.timeStampMs = ts;
        this.rackID = rackID;
        this.averageTemperature = averageTemperature;
    }

    public TemperatureAlert() {
        this(-1,-1,-1);
    }   // ????

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }
    public int getRackID() {
        return rackID;
    }

    public void setTimeStampMs(long timeStampMs) { this.timeStampMs = timeStampMs; }
    public long getTimeStampMs() { return timeStampMs; }

    public double getAverageTemperature() { return averageTemperature; }
    public void setAverageTemperature(double averageTemperature) { this.averageTemperature = averageTemperature; }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureAlert) {
            TemperatureAlert other = (TemperatureAlert) obj;
            return rackID == other.rackID;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return rackID;
    }

    @Override
    public String toString() {
        Date date = new Date(getTimeStampMs());
        // to print the date in normal format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //return "TemperatureAlert(" + getRackID() + ")";
        return sdf.format(date) + ",temperatureALERT," + getRackID() + "," + getAverageTemperature();
    }
}
