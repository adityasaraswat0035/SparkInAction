import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class MssqlToDatasetApp {
    public void start() {
        //Step 1 Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .appName("MSSQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();
        //Step2 Create Properties for MSSQL Dialect
        Properties props = new Properties(); //Creates a Properties object, which is going to be used to collect the properties needed
        props.setProperty("username", "admin"); //username property
        //Note always read password from env variables
//        byte[] password = System.getenv("DB_PASSWORD").getBytes();
//        props.put("password", new String(password));
//        password = null;
        props.setProperty("password", "admin"); //password property
        props.setProperty("databaseName", "Payments"); //database name specific to MSSQL dialect
        props.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"); //driver property
        //Step 3: Load Dataset from database
        Dataset<Row> df=sparkSession.read()
                        .jdbc("jdbc:sqlserver://localhost", //database url
                                "dbo.OutgoingPayment",//table name
                                props); //dialect properties
        df=df.orderBy(df.col("LastUpdatedAt"));
        df.show(10);
        df.schema().printTreeString();
        System.out.println("The dataframe contains " +
                df.count() + " record(s).");
    }
}
