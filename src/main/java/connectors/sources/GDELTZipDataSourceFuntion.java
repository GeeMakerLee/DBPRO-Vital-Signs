package connectors.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Read a directory that contains GDEL files (*.csv.gz), sort it according to the file name
 * and extract line by line from each .csv.gz GDELT file and returns the lines
 * we need to ensure that the lines returned are in order of occurrence...
 * input: path directory and extension of files to read
 */
public class GDELTZipDataSourceFuntion implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    private String directoryPathName;
    private String filesExtension;

    /*
    During  initialisation read the list of files in the directory
    order the list according to name, the name has date and time, so to read according
    to the time they were created
     */
    public GDELTZipDataSourceFuntion(String directoryPathName) {
        this.directoryPathName = directoryPathName;
    }

    public void run(SourceContext<String> sourceContext){
        List<Path> pathList = new ArrayList<>();
        File fileDirectoryPath = new File(directoryPathName);
        // Get the files of the directory sorted by name
        getFileNames(pathList, fileDirectoryPath);
        // We need to sort the files... to get the data in order
        pathList.sort(Comparator.naturalOrder());
        System.out.println(" Number of files to process: " + pathList.size());

        // open each file and get line by line
        String line;
        try {
            for(Path path : pathList) {
                // assuming here that the compressed files have extension .zip
                // in this case we need to replace .zip by .csv
                String csvFileName = path.getFileName().toString().replace(".zip", ".csv");

                //System.out.println("  FILE: " + path.toString() + "  csvFileName: " + csvFileName + "   ");
                ZipFile zipFile = new ZipFile(path.toString());
                // the zip entry for gdelt has the same name as the file, whitout .zip
                ZipEntry entry = zipFile.getEntry(csvFileName);
                BufferedReader br = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry), "UTF-8"));
                // Now get the lines
                while ((line = br.readLine()) != null) {
                    String rawData = line;
                    // return line by line
                    sourceContext.collect(rawData);
                }
                br.close();
                zipFile.close();
            }
        } catch (IOException e) {
                e.printStackTrace();
        }
    }

    public void cancel() {
        this.isRunning = false;
    }

    /**
     *
     */
    private List<Path> getFileNames(List<Path> pathList, File fileDirectoryPath) {
        try {
            File[] fileNames = fileDirectoryPath.listFiles();
            for (File csv : fileNames) {
               if(csv.isDirectory()) {
                  getFileNames(pathList, csv);
               } else {
                  pathList.add(csv.toPath());
               }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return pathList;
    }

}
