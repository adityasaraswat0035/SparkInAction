package Program;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class IngestionSchemaManipulationApp {
    public void start() {
        //Settings
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        //Step 1: Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local[*]")
                .getOrCreate();

        //Step 2: Create a dataframe=Dataset<Row>
        Dataset<Row> df = sparkSession.read().format("csv") //Dataframe reader need to read csv file
                .option("header", true) //csv file contains header
                .load("Chapter03/src/main/resources/data/Restaurants_in_Wake_County.csv"); //Name of the file in the data directory

        //Step 3: Show 5 Record;
        // df.show(5);

        //Step 4: Show Schema to StdOut
        //df.printSchema();

        //Step 5: Count No of Record in Dataframe i.e no of rows
        System.out.println("total records:" + df.count());

        //Step 6: Transform Dataframe Schema so that we can do union
        df =    df.withColumnRenamed("HSISID", "datasetid")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumn("country", lit("Wake"))
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumn("dateEnd",lit(null))
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID").drop("PERMITID").drop("GEOCODESTATUS");
        df=df.withColumn("id",
                concat(
                        col("state"),
                        lit("_"),
                        col("country"),
                        lit("_"),
                        col("datasetid")
                ));
        System.out.println("***********-Dataframe Transformed-***************");
        //Step 7: Show 5 row
        df.show(5);
        //Step 8: Print transformed schema to Stdout
        df.printSchema();

        //Step 9: Find out number of partitions in our physical storage
        System.out.println("***************--Looking for Partitions--********************");
        Partition [] partitions=df.rdd().partitions();
        int totalPartitions=partitions.length;
        System.out.println("Partition count before repartition: "+totalPartitions);

        //Step 10 :repartition the df to use four partitions
        df=df.repartition(4);
        System.out.println("Partition Count after repartition: "+df.rdd().partitions().length);

        //Step 11 Another way to see schema its print schema tree as we did using printSchema
        StructType schema=df.schema();
        schema.printTreeString();
        //Step 12: Schema as String
        String schemaAsString=schema.mkString();
        System.out.println("*** Schema as string: " + schemaAsString);

        //Step 13: Schema as prettyJson
        String schemaAsJson=schema.prettyJson();
        System.out.println("*** Schema as JSON: " + schemaAsJson);


    }
}
