package consumers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class ReadLinesFromFileStreamApp {
    private static Logger log = LoggerFactory
            .getLogger(ReadLinesFromFileStreamApp.class);

    public void start() throws TimeoutException {
        log.debug("-> start()");
        //Step 1: Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Read lines over a file stream")
                .master("local")
                .getOrCreate();
        StructType schema = new StructType()
                .add("fname", DataTypes.StringType)
                .add("mname", DataTypes.StringType)
                .add("lname", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("ssn", DataTypes.StringType);
        log.debug("Spark session initiated");
        Dataset<Row> df = sparkSession.readStream().format("csv").schema(schema).
        load("C:/_selfLearning/Sparks/MySpark/Chapter10/src/generated/");

        log.debug("Dataframe read from stream");
        StreamingQuery query = df.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .option("truncate", false)
                .option("numRows", 3)
                .start();
        log.debug("Query ready");
        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException ex) {
            ex.printStackTrace();
        }
    }
}
