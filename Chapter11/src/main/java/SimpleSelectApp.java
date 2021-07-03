import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleSelectApp {
    private Logger logger = Logger.getLogger(SimpleSelectApp.class);

    public void start() {
        //Step 1: Get a spark session by connecting to master on local
        SparkSession session = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();
        logger.debug("Spark session created");
        //Step 2: Define the schema for the csv so to load only nesseary column
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo",DataTypes.StringType,true),
                DataTypes.createStructField("yr1980",DataTypes.DoubleType,true),
        });
        logger.debug("Schema defined");
        //Step 3: load csv  parts to worker nodes
        Dataset<Row> dataframe = session.read()
                .format("csv")
                .schema(schema)
                .option("header", true)
                .load("Chapter11/src/main/resources/data/populationbycountry19802010millions.csv");
        logger.debug("Csv loaded successfully");
        //Step 4: Create View to Dataframe for current session
        dataframe.createOrReplaceTempView("geo_data");

        //Step 5: Show 5 countries whose population in 1980 yr was less then 1million
        Dataset<Row> smallCountries=session.sql("Select * from geo_data where yr1980<1 order by 2 limit 5");

        //Step 6: print schema  to console;
        smallCountries.printSchema();

        //Step 7: print dataframe to console.
        smallCountries.show(5,false);
        //Step 8: close spark session gracefully
        session.close();

    }
}
