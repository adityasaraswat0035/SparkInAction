import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class TextToDataframeApp {
    public void start() {
        //Step 1: Start a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Text to Dataframe")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read().format("text")//specify text when you want to ingest a text file.
                .load("Chapter07/src/main/resources/data/romeo-juliet-pg1777.txt");
        df.show(10);
        df.schema().printTreeString();
    }
}
