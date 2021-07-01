import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MSSQLWithJoinToDatasetApp {
    public void start() {
        //Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("MSSQL TO DATASET APP")
                .master("local")
                .getOrCreate();
        //Step 2: Prepare sql Query
        String sqlQuery = "Select top(20) og.OrderId as OrderId,pi.Body as Body " +
                " from dbo.OutgoingPayment og  inner join PaymentEntity pe on pe.Id=og.PaymentId" +
                " Inner join PaymentInstruction pi on pi.PaymentId=pe.publicId";
        //Read from Database
        Dataset<Row> df = sparkSession.read().format("jdbc")
                .option("url", "jdbc:sqlserver://localhost")
                .option("username", "admin")
                .option("password", "admin")
                .option("databaseName", "Payments")
                .option("dbtable", "(" + sqlQuery + ") paymentTable")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load();
        df.show(20);
        df.schema().printTreeString();
    }
}
