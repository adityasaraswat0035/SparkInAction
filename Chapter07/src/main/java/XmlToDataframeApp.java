import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class XmlToDataframeApp {
    public void start() {
        //Step 1: Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("XML to Dataframe")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read().format("xml") //specify XML as the format
                .option("rowTag", "row") //Element  or tag that  indicates a record in  the XML file
                .load("Chapter07/src/main/resources/data/nasa-patents.xml");
        df.show(5);
        df.schema().printTreeString();
    }
}
