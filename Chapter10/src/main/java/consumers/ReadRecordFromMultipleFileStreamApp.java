package consumers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class ReadRecordFromMultipleFileStreamApp {
    public void start() throws TimeoutException {
        //Step 1: Create a spark session
        SparkSession session = SparkSession.builder().appName("Read Record From Multiple FileStream App")
                .master("local").getOrCreate();
        //Step 2: if Schema is know lets define the schema
        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");
        //Step 3: Define streaming directories
        String stream1Directory = "dir1";
        String stream2Directory = "dir2";
        //Step 4: Create Dataframe from 1st stream
        Dataset<Row> dfStream1 = session.readStream().format("csv").load(stream1Directory);
        //Step 5: Create Dataframe from 2nd Stream
        Dataset<Row> dfStream2 = session.readStream().format("csv").load(stream2Directory);

        //Step 4:Start StreamingQuery for writing from 1st Stream
        StreamingQuery queryStream1 = dfStream1.writeStream().outputMode(OutputMode.Append())
                .format("console").foreach(new AgeChecker(1)).start();
        //Step 5:Start StreamingQuery for writing from 2nd Stream
        StreamingQuery queryStream2 = dfStream2.writeStream().outputMode(OutputMode.Append())
                .format("console").foreach(new AgeChecker(2)).start();
        long startProcessing = System.currentTimeMillis();
        int iterationCount = 0;
        //Step 6: running the stream for some duration here it is 1min
        while (queryStream1.isActive() && queryStream2.isActive()){
            iterationCount++;
            if(startProcessing+60000<System.currentTimeMillis()){
                queryStream1.stop();
                queryStream2.stop();
            }

        }


    }
}
