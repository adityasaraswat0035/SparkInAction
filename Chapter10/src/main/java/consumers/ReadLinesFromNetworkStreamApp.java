package consumers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class ReadLinesFromNetworkStreamApp {
    public void start() throws TimeoutException, StreamingQueryException {
        //Step 1: create a spark session
        SparkSession session = SparkSession.builder()
                .appName("Read lines from Network Stream")
                .master("local").getOrCreate();
        //Step 2: Load Datatset from network stream
        Dataset<Row> df = session.readStream().format("socket")
                .option("hosts", "localhost")
                .option("port", "8092")
                .load();
        //Step 3: Now we need to write this stream
        StreamingQuery query = df.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start(); //start the stream
        //Now we need to await the streaming
        query.awaitTermination(60000);


    }
}
