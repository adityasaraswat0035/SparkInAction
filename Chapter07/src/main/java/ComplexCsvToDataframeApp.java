import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ComplexCsvToDataframeApp {
    public void start() {
        //Step1 : Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .option("sep", ";")
                .option("multiline", true)
                .option("quote", "*")
                .option("dateFormat", "M/d/y")
                .option("inferschema", true)
                .load("Chapter07/src/main/resources/data/books.csv");
        System.out.println("Excerpt of dataframe content");
        df.show(7, 90);
        System.out.println("Dataframe's schema");
        df.schema().printTreeString();


    }
}
