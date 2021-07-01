package Program;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayToDatasetApp {
    public static void main(String[] args) {
        //Settings
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        //Step 1: connect to master and get spark session
        SparkSession session = SparkSession.builder()
                .appName("Array To Dataset App")
                .master("local")
                .getOrCreate();
        //Step 2: Create  array with 4 value
        String[] stringList =
                new String[]{"Jean", "Liz", "Pierre", "Lauric"};
        //Step 3: Convert the Array to list
        List<String> data = Arrays.asList(stringList);
        //Step 4: Create dataset of string from list and specify the encoders
        Dataset<String> ds = session.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();
        Dataset<Row> df=  ds.toDF();
        StructType schema=df.schema();
        df.show(5);
        schema.printTreeString();
    }
}
