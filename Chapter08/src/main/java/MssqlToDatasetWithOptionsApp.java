import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MssqlToDatasetWithOptionsApp {
    public void start() {
        //Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("MSSQL TO DATASET APP")
                .master("local")
                .getOrCreate();
        //Read from Database
        Dataset<Row> df = sparkSession.read().format("jdbc")
                .option("url", "jdbc:sqlserver://localhost")
                .option("username", "admin")
                .option("password", "admin")
                .option("databaseName", "Payments")
                .option("dbtable", "dbo.OutgoingPayment")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load();
        df.show(15);
        df.schema().printTreeString();

    }
}
