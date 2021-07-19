import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class PointsPerOrderApp {
    public void start() {
        //Step 1: Create spark session at local master
        SparkSession session = SparkSession.builder()
                .appName("Orders loyalty point")
                .master("local[*]")
                .getOrCreate();
        //Step 2: Register user define aggregate function
//        session.udf().register("pointAttribution", new PointAttributionUdaf());
        session.udf().register("pointAttribution",udaf(new PointAttributionUdaf(), Encoders.INT()));

        //Step 3: Load the dataset from csv
        Dataset<Row> df = session.read().format("csv")
                .option("header", true)
                .option("inferschema", true)
                .load("Chapter15/data/orders/orders.csv");
        //Step 4: Perform aggregations
        Dataset<Row> pointDf = df.groupBy(col("firstname")
                , col("lastname")
                , col("state"))
                .agg(sum(col("quantity")), callUDF("pointAttribution"
                        , col("quantity")).as("point"));

        pointDf.show(10);
    }
}
