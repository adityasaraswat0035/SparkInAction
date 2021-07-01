import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroToDataframeApp {
    public void start() {
        //Step 1: Create as spark session
        SparkSession sparkSession = SparkSession.builder().appName("Avro to Dataframe")
                .master("local")
                .getOrCreate();

        //Step 2: Load Data frame from avro file no need of any import file will automatically added
        //just need to add maven dependency
        Dataset<Row> df = sparkSession.read().format("avro")//Specifies the format
                .load("Chapter07/src/main/resources/data/weather.avro");
        //Step 3: Show records and Schema
        df.show(10);
        df.schema().printTreeString();
        System.out.println("The dataframe has " + df.count()
                + " rows.");

    }
}
