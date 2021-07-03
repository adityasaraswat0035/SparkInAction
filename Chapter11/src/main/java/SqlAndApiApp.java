import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class SqlAndApiApp {
    public void start() {
        //Step 1: Spark Sesion
        SparkSession session = SparkSession.builder()
                .appName("Simple SQL")
                .master("local")
                .getOrCreate();
        //Step 2: Create csv schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1981", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1982", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1983", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1984", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1985", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1986", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1987", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1988", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1989", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1990", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1991", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1992", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1993", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1994", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1995", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1996", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1997", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1998", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr1999", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2000", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2001", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2002", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2003", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2004", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2005", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2006", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2007", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2008", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2009", DataTypes.DoubleType, true),
                DataTypes.createStructField("yr2010", DataTypes.DoubleType, true),
        });
        Dataset<Row> dataframe = session.read()
                .format("csv")
                .schema(schema)
                .option("header", true)
                .load("Chapter11/src/main/resources/data/populationbycountry19802010millions.csv");


        for (int i = 1981; i < 2010; i++) {
            dataframe = dataframe.drop("yr" + i);
        }

        dataframe = dataframe.withColumn("evolution",
                expr("round((yr2010-yr1980)*1000000)"));
        dataframe.createOrReplaceTempView("geodata");

        Dataset<Row> negativeEvolutionDf = session.sql("SELECT * FROM geodata " +
                "where geo is not null and evolution<=0 order by evolution limit 25 ");
        negativeEvolutionDf.show(15, false);
        Dataset<Row> moreThanAMillionDf = session.sql("SELECT * FROM geodata " +
                "where geo is not null and evolution>999999 order by evolution limit 25 ");
        moreThanAMillionDf.show(15,false);
    }
}
