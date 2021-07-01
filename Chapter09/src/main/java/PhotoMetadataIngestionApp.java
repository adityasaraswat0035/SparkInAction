import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PhotoMetadataIngestionApp {
    public void start() {
        //Step 1: Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local")
                .getOrCreate();
        String importDirectory = "Chapter09/src/main/resources/data";
        Dataset<Row> df = sparkSession.read()
                .format("exif") //we specify the exif format to the reader
                //options specific to exif
                .option("recursive", "true") //able to read recursively through the directories
                .option("limit", "100000") //we want to limit to 100,000 files
                .option("extensions", "jpg,jpeg") //the data source will read only files with the JPG and JPEG format
                .load(importDirectory);//datasource need to know from which directory to start importing data
        //Once dataframe is generated we can utilize the dataframe api
        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);


    }
}
