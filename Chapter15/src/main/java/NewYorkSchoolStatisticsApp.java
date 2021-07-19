import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.spark.sql.functions.*;

@NotThreadSafe
public class NewYorkSchoolStatisticsApp implements AutoCloseable {
    private SparkSession session;

    public void start() {
        //Step 1: get Spark session from local master
        session = SparkSession.builder()
                .appName("NYC schools analytics")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> masterDf = loadDataUsing2018Format(
                "Chapter15/data/nyc_school_attendance/2018*.csv");
        masterDf = masterDf.unionByName(loadDataUsing2015Format(
                "Chapter15/data/nyc_school_attendance/2015*.csv"));
        masterDf = masterDf.unionByName(loadDataUsing2006Format(
                "Chapter15/data/nyc_school_attendance/200*.csv",
                "Chapter15/data/nyc_school_attendance/2012*.csv"
        ));
        // masterDf.show(10, false);
        //Dataset<Row> uniqueSchoolsDf = masterDf.select("schoolId").distinct();
        //Step 3:compute the average enrollment for each NYC school
        Dataset<Row> averageEnrollmentDf = masterDf.groupBy(
                col("schoolId")
                , col("schoolYear")
        ).avg("enrolled", "present", "absent").orderBy("schoolId", "schoolYear");
        //averageEnrollmentDf.show(10, false);
        Dataset<Row> studentCountPerYearDf = averageEnrollmentDf
                .withColumnRenamed("avg(enrolled)", "enrolled")
                .groupBy(col("schoolYear"))
                .agg(sum("enrolled").as("enrolled"))
                .withColumn("enrolled", floor("enrolled").cast(DataTypes.LongType))
                .orderBy("schoolYear");
        //studentCountPerYearDf.show(20);
        Row maxStudentRow = studentCountPerYearDf.orderBy(col("enrolled").desc()).first();
        String year = maxStudentRow.getAs("schoolYear");
        long max = maxStudentRow.getAs("enrolled");
        System.out.println(year + " was the year with most students, "
                + "the district served " + max + " students.");
        Dataset<Row> relativeStudentCountPerYearDf = studentCountPerYearDf
                .withColumn("max", lit(max))
                .withColumn("delta", expr("max-enrolled"))
                .drop("max")
                .orderBy("schoolYear");
        relativeStudentCountPerYearDf.show(20);
        Dataset<Row> maxEnrolledPerSchooldf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .max("enrolled")
                .orderBy("schoolId", "schoolYear");
        maxEnrolledPerSchooldf.show(20);
        Dataset<Row> minAbsenteeDf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .min("absent")
                .orderBy("schoolId", "schoolYear");
        minAbsenteeDf.show(20);

        Dataset<Row> absenteeRatioDf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .agg(
                        max("enrolled").alias("enrolled"),
                        avg("absent").as("absent"));
        absenteeRatioDf = absenteeRatioDf
                .groupBy(col("schoolId"))
                .agg(
                        avg("enrolled").as("avg_enrolled"),
                        avg("absent").as("avg_absent"))
                .withColumn("%", expr("avg_absent / avg_enrolled * 100"))
                .filter(col("avg_enrolled").$greater(10))
                .orderBy("%");
        absenteeRatioDf.show(5);
        absenteeRatioDf
                .orderBy(col("%").desc())
                .show(5);
    }

    private Dataset<Row> loadDataUsing2018Format(String... fileNames) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "schoolId",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "date",
                        DataTypes.DateType,
                        false),
                DataTypes.createStructField(
                        "enrolled",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "present",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "absent",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "released",
                        DataTypes.IntegerType,
                        false)});
        Dataset<Row> df = session.read().format("csv")
                .option("header", true)
                .option("dateFormat", "yyyyMMdd")
                .schema(schema)
                .load(fileNames);
        return df
                .withColumn("schoolYear", lit(2018));
    }

    private Dataset<Row> loadDataUsing2015Format(String... fileNames) {
        return loadData(fileNames, "MM/dd/yyyy");
    }

    private Dataset<Row> loadDataUsing2006Format(String... filenames) {
        return loadData(filenames, "yyyyMMdd");
    }

    private Dataset<Row> loadData(String[] filenames, String dateFormat) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("schoolId", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.StringType, false),
                DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
                DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
                DataTypes.createStructField("present", DataTypes.IntegerType, false),
                DataTypes.createStructField("absent", DataTypes.IntegerType, false),
                DataTypes.createStructField("released", DataTypes.IntegerType, false),
        });
        Dataset<Row> df = session.read().format("csv").option("header", true)
                .option("dateformat", dateFormat)
                .schema(schema)
                .load(filenames);
        return df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4));
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }
}
