import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class OpenedLibrariesApp {
    public void start() {
        //Step 1: create spark session on local master
        SparkSession session = SparkSession.builder().appName("Custom UDF to check if in range")
                .master("local[*]").getOrCreate();
        //Step 2:Registering the UDF in spark
        session.udf().register("isOpen",new IsOpenUdf(),DataTypes.BooleanType);

        //Step 3: Ingest the csv data
        Dataset<Row> librariesDf = session.read().format("csv").option("header", true)
                .option("inferSchema", true)
                .option("encoding", "cp1252")
                .load("Chapter14/data/south_dublin_libraries/sdlibraries.csv");
        librariesDf=librariesDf.drop("Administrative_Authority")
                .drop("Address1")
                .drop("Address2")
                .drop("Town")
                .drop("Postcode")
                .drop("County")
                .drop("Phone")
                .drop("Email")
                .drop("Website")
                .drop("Image")
                .drop("WGS84_Latitude")
                .drop("WGS84_Longitude");
        librariesDf.show(false);
        librariesDf.schema().printTreeString();

        Dataset<Row> dateTimedf=createDataframe(session);
        dateTimedf.show();
        dateTimedf.printSchema();

        Dataset<Row> df=librariesDf.crossJoin(dateTimedf);
        Dataset<Row> finalDf=df.withColumn("open",callUDF("isOpen"
                ,col("Opening_Hours_Monday")
                ,col("Opening_Hours_Tuesday")
                ,col("Opening_Hours_Wednesday")
                ,col("Opening_Hours_Thursday")
                ,col("Opening_Hours_Friday")
                ,col("Opening_Hours_Saturday")
                ,lit("Closed")
                ,col("date")))
                .drop("Opening_Hours_Monday")
                .drop("Opening_Hours_Tuesday")
                .drop("Opening_Hours_Wednesday")
                .drop("Opening_Hours_Thursday")
                .drop("Opening_Hours_Friday")
                .drop("Opening_Hours_Saturday");

        finalDf.show();

    }

    private Dataset<Row> createDataframe(SparkSession session) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("date_str", DataTypes.StringType, false)
        });
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("2019-03-11 14:30:00"));
        rows.add(RowFactory.create("2019-04-27 16:00:00"));
        rows.add(RowFactory.create("2020-01-26 05:00:00"));
        return session.createDataFrame(rows, schema).withColumn("date",
                to_timestamp(col("date_str"))).drop("date_str");
    }
}
