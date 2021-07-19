import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class HigherEdInstitutionPerCountyApp {
    public void start() {
        //Step 1 Connect to master on local node by Spark Session
        SparkSession session = SparkSession.builder().appName("Join")
                .master("local[*]").getOrCreate();

        Dataset<Row> censusDf = censusDataframe(session);
        Dataset<Row> higherEdf = higherEdfDataFrame(session);
        Dataset<Row> countryZipDf = countryZipDataframe(session);
        Dataset<Row> institutePerCountryDf = higherEdf.join(countryZipDf,
                higherEdf.col("zip").equalTo(countryZipDf.col("zip")),
                "inner"
        );
        //Note it will drop both column of zip so we need to specifiy the source
        //institutePerCountryDf=  institutePerCountryDf.drop("zip");
        institutePerCountryDf = institutePerCountryDf.drop(higherEdf.col("zip"));
        institutePerCountryDf = institutePerCountryDf.join(censusDf,
                institutePerCountryDf.col("county").equalTo(censusDf.col("countryId"))
                , "left"
        );
        institutePerCountryDf = institutePerCountryDf.drop("county").drop("countryId");
        institutePerCountryDf.schema().printTreeString();
        institutePerCountryDf.show(10, false);
    }

    private Dataset<Row> countryZipDataframe(SparkSession session) {
        //Step 2: load data from csv into partitions created on worker node
        Dataset<Row> countryZipDf = session.read().format("csv")
                .option("header", true)
                .option("inferschema", true)
                .load("Chapter12/data/hud/COUNTY_ZIP_092018.csv");
        //Step 3: Drop unnecessary column from data frame
        countryZipDf = countryZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");
        System.out.println("Country zip data");
        return countryZipDf;

//        countryZipDf.schema().printTreeString();
//        countryZipDf.show(10,false);
    }

    private Dataset<Row> higherEdfDataFrame(SparkSession session) {
        //Step 2: load the Dataframe
        Dataset<Row> higherEdf = session.read().format("csv")
                .option("header", true)
                .option("inferschema", true)
                .load("Chapter12/data/dapip/InstitutionCampus.csv");
        //Step 2: Filter on the institution
        higherEdf = higherEdf.filter("LocationType='Institution'");

        //Step 3:Rename locationName to Location
        higherEdf = higherEdf.withColumnRenamed("LocationName", "location");
        //Step 3: Split Address field based on space
        higherEdf = higherEdf.withColumn("addressElement",
                split(col("Address"), " "));
        //Step 4: Get the count of addressElement
        higherEdf = higherEdf.withColumn("addressElementCount",
                size(col("addressElement")));
        //Step 5: Extract 9 digit zip code(last element)
        higherEdf = higherEdf.withColumn("zip9",
                element_at(col("addressElement"), col("addressElementCount")));
        //Step 6: Extract first 5 digit of zipcode
        higherEdf = higherEdf.withColumn("zip", split(col("zip9"), "-").getItem(0));

        //Step 7: Clean up
        higherEdf = higherEdf.drop("DapipId")
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("LocationType")
                .drop("Address")
                .drop("GeneralPhone")
                .drop("AdminName")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .drop("Fax")
                .drop("UpdateDate")
                .drop("addressElement")
                .drop("addressElementCount")
                .drop("zip9");
        System.out.println("higher education institution data");
//        higherEdf = higherEdf.sample(0.1);
//        higherEdf.show(10, false);
        return higherEdf;
    }

    private Dataset<Row> censusDataframe(SparkSession session) {
        //Step 2: Load the Data
        Dataset<Row> censusDf = session.read()
                .format("csv")
                .option("header", true)
                .option("inferschema", true)
                .option("encoding", "cp1252")
                .load("Chapter12/data/census/PEP_2017_PEPANNRES.csv");
        censusDf = censusDf.drop("GEO.id")
                .withColumnRenamed("GEO.id2", "countryId")
                .withColumnRenamed("GEO.display-label", "country")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017");
        //Step 3: Print Schema info and sampling data
        System.out.println("Census data");
//        censusDf.sample(0.1).show(10, false);
//        censusDf.schema().printTreeString();
        return censusDf;
    }
}
