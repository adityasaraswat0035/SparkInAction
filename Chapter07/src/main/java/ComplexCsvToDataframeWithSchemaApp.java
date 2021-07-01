import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ComplexCsvToDataframeWithSchemaApp {
    public void start() {
        //Step 1: Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local")
                .getOrCreate();
        //Step 2: Create a schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("authordId", DataTypes.IntegerType, true),
                DataTypes.createStructField("bookTitle", DataTypes.StringType, false),
                DataTypes.createStructField("releaseDate", DataTypes.DateType, true),
                DataTypes.createStructField("url", DataTypes.StringType, false),
        });
        //Step 3: Load the csv
        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .option("sep", ";")
                .option("multiline", true)
                .option("dateformat", "MM/dd/yyyy")
                .option("quote", "*")
//                .option("inferschema",true) //as we have schema already

                .schema(schema) //as we already defined the schema it will tell reader to use this schema
                .load("Chapter07/src/main/resources/data/books.csv");

        System.out.println("Excerpt of dataframe content");
        df.show(7, 90);
        System.out.println("Dataframe's schema");
        df.schema().printTreeString();


    }
}
