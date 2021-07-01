package Program;

import org.apache.spark.sql.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class CsvToRelationalDatabaseApp {
    public void start() {
        //Set hadoop home dir path
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        //Log4j Setting to log only for warn
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        //Step1: Create a spark session to master;
        SparkSession session = SparkSession.builder().appName("CSV to DB").master("local[*]")
                .getOrCreate();
        //Step 2: Ingest CSV file
        //Reads a CSV file with header, called authors.csv, and stores it in a dataframe
        Dataset<Row> dataframe = session.read()
                .option("header", true)
                .format("csv")
                //Csv file has header
                //.csv("Chapter02/src/main/resources/data/authors.csv");
                .load("Chapter02/src/main/resources/data/authors.csv");
        //Step 3 Creates a new column called “name” as the
        // concatenation of lname, a virtual column containing “, ” and the fname column
       dataframe= dataframe.withColumn("name",
                concat(   //Both concat() and lit() were statically imported. from functions
                                dataframe.col("lname"),
                        lit(", "),
                        dataframe.col("fname")
                ));

        //The connection URL, assuming your sqlserver instance runs locally on the default port,
        // and the database you use is SparkLearning.

        String dbConnectionUrl="jdbc:sqlserver://localhost";
        Properties properties=new Properties();
        properties.setProperty("user","admin");
        properties.setProperty("password","admin");
        properties.setProperty("databaseName","SparkLearning");
        properties.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver");
//        properties.setProperty("username","admin");
//        properties.setProperty("password","admin");

        //Overwrites in a table called author
        dataframe.write().mode(SaveMode.Overwrite).jdbc(dbConnectionUrl,"author",properties);
        System.out.println("Process complete");
        session.close();
    }
}
