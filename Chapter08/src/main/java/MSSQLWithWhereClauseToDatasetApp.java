import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MSSQLWithWhereClauseToDatasetApp {
    public void start() {
        //Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("MSSQL TO DATASET APP")
                .master("local")
                .getOrCreate();
        //Step 2: Prepare sql Query
        String sqlQuery = "Select * from dbo.OutgoingPayment where orderId='WPWGNHU18EFR'";
        //Read from Database
        Dataset<Row> df = sparkSession.read().format("jdbc")
                .option("url", "jdbc:sqlserver://localhost")
                .option("username", "admin")
                .option("password", "admin")
                .option("databaseName", "Payments")
                .option("dbtable", "(" + sqlQuery + ") paymentTable")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load();
        df.show(15);
        df.schema().printTreeString();
    }
}
