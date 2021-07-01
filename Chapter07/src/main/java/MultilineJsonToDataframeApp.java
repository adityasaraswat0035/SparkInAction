import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MultilineJsonToDataframeApp {
    public void start() {
        //Step 1 Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();
        //Step 2: Read multiline json file
        Dataset<Row> df = sparkSession.read()
                .format("json")
                .option("multiline", true) //The key to processing  multiline JSON!
                .load("Chapter07/src/main/resources/data/countrytravelinfo.json");
        df.show(3);
        df.schema().printTreeString();
    }
}
