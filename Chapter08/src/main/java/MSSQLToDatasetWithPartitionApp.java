import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MSSQLToDatasetWithPartitionApp {
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
                    .option("partitionColumn","PaymentId") //column to partition on
                    .option("lowerBound", "1") //lowebound of stride
                    .option("upperBound","1000") //upper bound of stride
                    .option("numPartitions",10)//No of Partitions
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .load();
            System.out.println("Number of Partitions"+df.rdd().partitions().length);
            df.show(15);
            df.schema().printTreeString();
    }
}
