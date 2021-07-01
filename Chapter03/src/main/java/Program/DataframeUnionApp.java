package Program;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DataframeUnionApp {
    public static void main(String[] args) {
        //Settings
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        //Step 1: connect to master and get spark session
        SparkSession session = SparkSession.builder()
                .appName("Dataframe union")
                .master("local")
                .getOrCreate();
        Dataset<Row> wakeRestaurantsDf =buildWakeRestaurantsDataframe(session);
        wakeRestaurantsDf=wakeRestaurantsDf.repartition(3);
        System.out.println("No of Record for wakeRestaurantsDf: "+wakeRestaurantsDf.count());
        System.out.println("No of Partitions for wakeRestaurantsDf: "+wakeRestaurantsDf.rdd().partitions().length);
        Dataset<Row> durhamRestaurantsDf = buildDurhamRestaurantsDataframe(session);
        durhamRestaurantsDf= durhamRestaurantsDf.repartition(2);
        System.out.println("No of Record for durhamRestaurantsDf: "+durhamRestaurantsDf.count());
        System.out.println("No of Partitions for durhamRestaurantsDf: "+durhamRestaurantsDf.rdd().partitions().length);
        //Step 6: Union the dataframes
        Dataset<Row> combinedDataFrame= combineDataframes(wakeRestaurantsDf,durhamRestaurantsDf);
        System.out.println("No of Record for combinedDataFrame: "+combinedDataFrame.count());
        System.out.println("No of Partitions for combinedDataFrame: "+combinedDataFrame.rdd().partitions().length);

    }

    private static Dataset<Row> combineDataframes(Dataset<Row> wakeRestaurantsDf,
                                                  Dataset<Row> durhamRestaurantsDf) {
        return durhamRestaurantsDf.unionByName(wakeRestaurantsDf);
    }

    private static Dataset<Row> buildDurhamRestaurantsDataframe(SparkSession session) {
        //Step 4: Ingest Json Data
        Dataset<Row> jsonDf= session.read().format("json")
                .load("Chapter03/src/main/resources/data/Restaurants_in_Durham_County_NC.json");
        //Step 5: Transform the Json Dataframe
        jsonDf= jsonDf.withColumn("country", lit("Durham"))
                .withColumn("datasetId", jsonDf.col("fields.id"))
                .withColumn("name", jsonDf.col("fields.premise_name"))
                .withColumn("address1", jsonDf.col("fields.premise_address1"))
                .withColumn("address2", jsonDf.col("fields.premise_address2"))
                .withColumn("city", jsonDf.col("fields.premise_city"))
                .withColumn("state", jsonDf.col("fields.premise_state"))
                .withColumn("zip", jsonDf.col("fields.premise_zip"))
                .withColumn("tel", jsonDf.col("fields.premise_phone"))
                .withColumn("dateStart", jsonDf.col("fields.opening_date"))
                .withColumn("dateEnd", jsonDf.col("fields.closing_date"))
                .withColumn("type",
                        split(
                                jsonDf.col("fields.type_description"), "-"
                        ).getItem(1))
                .withColumn("geoX", jsonDf.col("fields.geolocation").getItem(0))
                .withColumn("geoY", jsonDf.col("fields.geolocation").getItem(1));
        jsonDf = jsonDf.withColumn("id", concat(
                jsonDf.col("state"),
                lit("-"),
                jsonDf.col("country"),
                lit("-"),
                jsonDf.col("datasetId")
        ));
        jsonDf=jsonDf.drop("fields").drop("geometry").drop("record_timestamp").drop("recordid");
        return jsonDf;
    }

    private static  Dataset<Row> buildWakeRestaurantsDataframe(SparkSession session) {
        //Step 2: CSV ingestion
        Dataset<Row> csvDf = session.read().format("csv").option("header", true)
                .load("Chapter03/src/main/resources/data/Restaurants_in_Wake_County.csv");

        //Step 3:Transform the CSV dataframe
        csvDf = csvDf.withColumnRenamed("HSISID", "datasetid")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumn("country", lit("Wake"))
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumn("dateEnd", lit(null))
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID").drop("PERMITID").drop("GEOCODESTATUS");
        csvDf = csvDf.withColumn("id",
                concat(
                        col("state"),
                        lit("_"),
                        col("country"),
                        lit("_"),
                        col("datasetid")
                ));
        return  csvDf;
    }
}
