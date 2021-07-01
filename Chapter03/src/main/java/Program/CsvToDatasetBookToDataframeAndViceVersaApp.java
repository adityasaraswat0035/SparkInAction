package Program;

import Program.Mappers.BookMapper;
import Program.Pojo.Book;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDatasetBookToDataframeAndViceVersaApp {
    public static void main(String[] args) {
        //Settings
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        //Step 1: connect to master and get spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Array To Dataset App")
                .master("local")
                .getOrCreate();
        //Step 2: Ingest a csv
        Dataset<Row> df = sparkSession.read().format("csv") //Dataframe reader need to read csv file
                .option("header", true) //csv file contains header
                .option("inferSchema",true)
                .load("Chapter03/src/main/resources/data/books.csv"); //Name of the file in the data directory
        //Step 3:
        System.out.println("*** Books ingested in a dataframe");
       df.show(5);
       df.schema().printTreeString();
       // Dataset<Book> books=df.map(new BookMapper(), Encoders.bean(Book.class));
       // books.show();

    }
}
