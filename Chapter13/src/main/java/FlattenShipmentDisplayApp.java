import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class FlattenShipmentDisplayApp {
    public void start() {
        //Step 1: get Spark session from master on local
        SparkSession session = SparkSession.builder()
                .appName("Flatenning JSON doc describing shipments")
                .master("local[*]")
                .getOrCreate();
        //Step2 load the json to partiton
        Dataset<Row> df = session.read()
                .format("json")
                .option("multiline", true)
                .load("Chapter13/data/json/shipment.json");
        //Step3: Transform the json
        df = df.withColumn("supplier_name", col("supplier.name"))
                .withColumn("supplier_city", col("supplier.city"))
                .withColumn("supplier_state", col("supplier.state"))
                .withColumn("supplier_country", col("supplier.country"))
                .drop("supplier")
                .withColumn("customer_name",col("customer.name"))
                .withColumn("customer_city",col("customer.city"))
                .withColumn("customer_state",col("customer.state"))
                .withColumn("customer_country",col("customer.country"))
                .drop("customer");

        //Step 4: Explode the books array
        df=df.withColumn("items",explode(col("books")));
        df=df.withColumn("qty",col("items.qty"))
                .withColumn("title",col("items.title"))
                .drop("items")
                .drop("books");
        df.show(10, false);
        df.schema().printTreeString();
        df.createOrReplaceTempView("shipment_details");
        Dataset<Row> bookCountDf= session.sql("select count(*) As total_count from shipment_details");
        bookCountDf.show(false);
    }
}
