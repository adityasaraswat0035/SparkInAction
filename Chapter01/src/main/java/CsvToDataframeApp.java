import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class CsvToDataframeApp {
    public void start() {
        // ->Step1 We need to create a spark session which will connect to Master Node and Then return Spark Session
        SparkSession session = SparkSession.builder().appName("CSV to Dataset")
                .master("local[*]").getOrCreate(); //->Create Spark session on local master
        Dataset<Row> dataframe = session.read().option("header", true) //-> Read a csv file with header and
                .csv("Chapter01/src/main/resources/data/books.csv");//Store the csv file in Dataset<Row> called
                                                                        //Dataframe
        dataframe.show(5); //->Show atmost five rows of dataframe
        session.close();
    }
}
