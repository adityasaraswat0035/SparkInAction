import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class RecordTransformationApp {
    public void start() {
        //Step 1 Connect to Master
        SparkSession sparkSession = SparkSession.
                builder()
                .appName("Record transformations")
                .master("local[*]")
                .getOrCreate();
        //Step 2 Load the csv with infer schema and get dataframe
        Dataset<Row> df = sparkSession
                .read().format("csv")
                .option("header", true)
                .option("inferschame", true)
                .load("Chapter12/data/census/PEP_2017_PEPANNRES.csv");
        //Step 3: Transform the data
        Dataset<Row> internmediateDf = df
                .drop("GEO.id")
                .withColumnRenamed("GEO.id2", "id")
                .withColumnRenamed("GEO.display-label", "label")
                .withColumnRenamed("rescen42010", "real2010")
                .drop("resbase42010")
                .withColumnRenamed("respop72010", "est2010")
                .withColumnRenamed("respop72011", "est2011")
                .withColumnRenamed("respop72012", "est2012")
                .withColumnRenamed("respop72013", "est2013")
                .withColumnRenamed("respop72014", "est2014")
                .withColumnRenamed("respop72015", "est2015")
                .withColumnRenamed("respop72016", "est2016")
                .withColumnRenamed("respop72017", "est2017");

        //Step 5: Transform and add new columns
//        internmediateDf = internmediateDf.withColumn("countryState", split(
//                col("label"), ","))
//                .withColumn("stateId", (col("id").cast(DataTypes.IntegerType))
//                        .divide(1000).cast(DataTypes.IntegerType))
//                .withColumn("countryId", (col("id").cast(DataTypes.IntegerType))
//                        .mod(1000));
        internmediateDf = internmediateDf.withColumn("countryState", split(
                col("label"), ","))
                .withColumn("stateId", expr("int(id/1000)"))
                .withColumn("countryId", expr("int(id%1000)"));


        //Step 6 : Separate Country and State
        internmediateDf = internmediateDf.withColumn("state", col("countryState").getItem(1))
                .withColumn("country", col("countryState").getItem(0))
                .drop("countryState");

        //Step 7: Print the schema
        internmediateDf.printSchema();

        //Step 8: perform the analysis
        Dataset<Row> statDf = internmediateDf
                .withColumn("diff", expr("est2010-real2010"))
                .withColumn("growth", expr("est2017-est2010"));


        //Start 9: remove unused column
       statDf= statDf.drop("id")
                .drop("label")
                .drop("real2010")
                .drop("est2010")
                .drop("est2011")
                .drop("est2012")
                .drop("est2013")
                .drop("est2014")
                .drop("est2015")
                .drop("est2016")
                .drop("est2017");

        //Step 10: print the schema
        statDf.schema().printTreeString();
        //Step 11: Sampling of Data
        statDf.sample(.01).show(10, false);
        //Step 12: Gracefully Shutdown the session
        sparkSession.close();
    }
}
