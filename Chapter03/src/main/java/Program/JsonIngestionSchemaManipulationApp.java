package Program;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class JsonIngestionSchemaManipulationApp {
    public void start() {
        //Settings
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        //Step 1: connect to master and get spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Restaurants in Durham County, NC")
                .master("local")
                .getOrCreate();
        //Step 2: Ingest JSON Data
        Dataset<Row> df = sparkSession.read()
                .format("json")
                .load("Chapter03/src/main/resources/data/Restaurants_in_Durham_County_NC.json");
        //Step 3: Show the first two record
        df.show(2);
        //Step 4: Show the Schema as well
        StructType schema = df.schema();
        schema.printTreeString();

        //Step 5: transformation: access nested field in structured schema is by . and array item form struct type
        //by getItem(0)
        df = df.withColumn("country", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type",
                        split(
                                df.col("fields.type_description"), "-"
                        ).getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));

        df = df.withColumn("id", concat(
                df.col("state"),
                lit("-"),
                df.col("country"),
                lit("-"),
                df.col("datasetId")
        ));
        //Step 6: Drop the unwanted column
        df=df.drop("fields").drop("geometry").drop("record_timestamp").drop("recordid");
        df.show(4);
        //Step 7: Show transformed Schema
        StructType transformedSchema=df.schema();
        transformedSchema.printTreeString();
        //Step 8 Show number of Partitions
        Partition[] partitions=df.rdd().partitions();
        int totalPartitions=partitions.length;
        System.out.println("Partition count before repartition: " +
                totalPartitions);
        df=df.repartition(4);
        totalPartitions=df.rdd().partitions().length;
        System.out.println("Partition count after repartition: " +
                totalPartitions);


        sparkSession.close();
    }
}
