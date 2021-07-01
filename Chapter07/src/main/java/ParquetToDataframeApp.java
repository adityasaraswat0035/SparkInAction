import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetToDataframeApp {
    public void start() {
        //Step 1: Create spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("parquet to Dataframe")
                .master("local")
                .getOrCreate();
        //Step 2: load parquet file as spark natively support that like csv
        Dataset<Row> df = sparkSession.read().format("parquet") //provided file format parquet
                .load("Chapter07/src/main/resources/data/alltypes_plain.parquet");
        df.show(10);
        df.schema().printTreeString();
    }
}
