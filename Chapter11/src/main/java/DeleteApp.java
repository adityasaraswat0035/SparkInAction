import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DeleteApp {
    public void start() {
        //Step 1: Spark Session
        SparkSession session = SparkSession.builder()
                .appName("Delete app").master("local")
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

        //Step 4: Create View to Dataframe for current session
        dataframe.createOrReplaceTempView("geo_data");
        System.out.println(String.format("Territories in orginal dataset: %d", dataframe.count()));

        Dataset<Row> cleanedDf = session.sql("select * from geo_data where geo is not null "
                + "and geo != 'Africa'"
                + "and geo != 'Africa' "
                + "and geo != 'North America' "
                + "and geo != 'World' "
                + "and geo != 'Asia & Oceania' "
                + "and geo != 'Central & South America' "
                + "and geo != 'Europe' "
                + "and geo != 'Eurasia' "
                + "and geo != 'Middle East' " +
                " order by yr2010 desc");

        System.out.println(String.format("Territories in cleaned dataset: %d", cleanedDf.count()));
        cleanedDf.show(20, false);

        Dataset<Row> table= session.table("geo_data");
        table.show(10,false);

    }
}
