package connectors.sources;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.lang.Thread.sleep;

public class DwdDataSourceFunction implements SourceFunction<Tuple3<String, Double, String>> {

    //MESS_DATUM,SDK.Sonnenscheindauer,TMK.Lufttemperatur,TXK.Lufttemperatur_Max,TNK.Lufttemperatur_Min
    // 2019-05-06,5.967,8.5,13.4,3.5
    // 2019-05-07,6.467,8.8,13.5,4.3
    // 2019-05-08,9.1,12.2,18.3,2.8
    // 2019-05-09,2.783,13.9,18.4,10.4
    // 2019-05-10,2.533,13.2,16.8,8.5

    private volatile boolean isRunning = true;

    private String dwdFile;  // wave file in .csv format
    /*
     Read the DWD file from the .csv file
     */
    public DwdDataSourceFunction(String fileName) {
        this.dwdFile = fileName;
    }

    public void run(SourceFunction.SourceContext<Tuple3<String, Double, String>> sourceContext) throws Exception {
        try {
            System.out.println("  FILE:" + dwdFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dwdFile), "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                // to introduce a delay on the stream
                try {
                       sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                }
                String[] data = line.split(",");
                Tuple3<String, Double, String> dwdData = new Tuple3<String, Double, String>("Lufttemperatur", Double.valueOf(data[2]), data[0]);
                sourceContext.collect(dwdData);
            }
            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void cancel() {
        this.isRunning = false;
    }
}

