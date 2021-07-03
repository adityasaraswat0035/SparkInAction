import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleSelectGlobalViewApp {
    public void start() throws AnalysisException {
        //Step 1: Spark Session
        SparkSession session = SparkSession.builder()
                .appName("Simple Select Global View App")
                .master("local")
                .getOrCreate();
        //Step 2: Create csv Schema
        StructType schema= DataTypes.createStructType(new StructField[]{
           DataTypes.createStructField("countries",DataTypes.StringType,true),
           DataTypes.createStructField("yr1980",DataTypes.DoubleType,true)
        });
        //Step 3:load the csv
        Dataset<Row> dataframe=session.read().format("csv").schema(schema)
                .option("header",true)
                .load("Chapter11/src/main/resources/data/populationbycountry19802010millions.csv");
        //Step 4: Create View to Dataframe for current session
        dataframe.createOrReplaceGlobalTempView("geo_data");
        //Step 5: Show 5 countries whose population in 1980 yr was less then 1million
        Dataset<Row> smallCountries=session.sql("Select * from global_temp.geo_data where yr1980<1 order by 2 limit 5");
        //Step 6: print schema  to console;
        smallCountries.printSchema();
        //Step 7: print dataframe to console.
        smallCountries.show(5,false);

        SparkSession newSession=session.newSession();
        Dataset<Row> slightlyBiggerCountriesDf =
                newSession.sql(
                        "SELECT * FROM global_temp.geo_data "
                                + "WHERE yr1980 > 1 ORDER BY 2 LIMIT 10");
        slightlyBiggerCountriesDf.show(10, false);
        //Step 8: close spark session gracefully
        session.close();
    }
}
