import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class JsonLinesToDataframeApp {
    public void start() {
        //Step 1: Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read().format("json")
                .load("Chapter07/src/main/resources/data/durham-nc-foreclosure-2006-2016.json");
        df.show(5, 13);
        df.schema().printTreeString();

    }
}
