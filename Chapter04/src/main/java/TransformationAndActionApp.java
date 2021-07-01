import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class TransformationAndActionApp {
    public void start(String mode) {
        //step :1 sets the timer;
        long t0 = System.currentTimeMillis();
        //Step 2: Create a spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")
                .master("local[*]")
                .getOrCreate();
        //Step 3: Measuring the time spent creating the session
        long t1 = System.currentTimeMillis();
        System.out.println("1. Creating Session--------------" + (t1 - t0));
        //Step 4: Ingest the CSV file
        Dataset<Row> dataframe = sparkSession.read().format("csv")
                .option("header", true)
                .load("Chapter04/src/main/resources/data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
        Dataset<Row> initialDf = dataframe;
        //Step 5: Time spend to load the dataframe;
        long t2 = System.currentTimeMillis();
        System.out.println("2. Loading initial dataset . ...." + (t2 - t1));

        //Step 6:Create a big dataset
        for (int i = 0; i < 8; i++) {
            dataframe = dataframe.union(dataframe);
        }
        //Step 7: Measure the time needed to make dataset larger
        long t3 = System.currentTimeMillis();
        System.out.println("3. Building full dataset ........ " + (t3 - t2));
        //Step 8: Clean up process
        dataframe = dataframe.withColumnRenamed("Lower Confidence Limit", "lcl");
        dataframe = dataframe.withColumnRenamed("Upper Confidence Limit", "ucl");
        //Step 9: Measure cleanup time
        long t4 = System.currentTimeMillis();
        System.out.println("4. Clean-up ..................... " + (t4 - t3));

        //Step 10: Actual data transformation with different mode
        //if mode is noop skip all transformation  otherwise creates new columns
        if (mode.compareToIgnoreCase("noop") != 0) {
            dataframe = dataframe.withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", col("lcl"))
                    .withColumn("ucl2", col("ucl"));
            if(mode.compareToIgnoreCase("full")==0){
                dataframe=dataframe.drop("lcl2").drop("ucl2").drop("avg");
            }
        }
        long t5 = System.currentTimeMillis();
        System.out.println("5. Transformations ............. " + (t5 - t4));

        //Step 11: Collect the result (action)
        dataframe.collect();
        long t6=System.currentTimeMillis();
        System.out.println("6. Final action ................. " + (t6 - t5));
        System.out.println("");
        System.out.println("# of records .................... " + dataframe.count());

        //Step 12: Explain a plan
        dataframe.explain();
    }
}
