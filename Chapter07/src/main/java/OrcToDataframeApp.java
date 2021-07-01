import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrcToDataframeApp {
    public void start() {
        //Step 1: Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Orc to Dataframe")
                .config("spark.sql.orc.impl", "native") //Use the native implementation
                                                        //to access the ORC file, not the
                                                        //Hive implementation.
                .master("local")
                .getOrCreate();
        //Step 2: Load orc file to data frame
        Dataset<Row> df = sparkSession.read().format("orc")
                .load("Chapter07/src/main/resources/data/demo-11-zlib.orc");
        df.show(10);
        df.schema().printTreeString();
        System.out.println("The dataframe has " + df.count() + " rows.");

    }
}
