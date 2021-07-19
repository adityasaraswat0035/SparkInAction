import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class OrderStatisticsApp {
    public void start() {
        //Step 1: Create spark session on local master
        SparkSession session = SparkSession.builder()
                .appName("Orders analytics")
                .master("local[*]")
                .getOrCreate();
        //Step 2: ingest the order data
        Dataset<Row> dataframe = session
                .read()
                .format("csv")
                .option("header", true)
                .option("inferschema", true)
                .load("Chapter15/data/orders/orders.csv");
        //Step 3: Performing aggregation using Dataframe API
        Dataset<Row> apidf=dataframe.groupBy(col("firstname")
                ,col("lastName")
                ,col("state")
        ).agg(sum(col("quantity")),sum(col("revenue")),avg(col("revenue")));
        apidf.show(20);

        //Step 4: PERFORMING AN AGGREGATION USING SPARK SQL
        dataframe.createOrReplaceTempView("orders");
        Dataset<Row> sqlDf=session.sql("SELECT firstname,lastname,state,sum(quantity),sum(revenue),avg(revenue)" +
                "" +
                " from orders group by firstname,lastName,state");
        sqlDf.show(20);

    }
}
