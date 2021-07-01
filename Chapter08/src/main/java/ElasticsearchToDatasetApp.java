import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class ElasticsearchToDatasetApp {
    public  void start(){
        long t0 = System.currentTimeMillis();
        //Step 1: Create a spark session
        SparkSession sparkSession= SparkSession.builder().appName("Elasticsearch to Dataframe")
                                    .master("local")
                                    .getOrCreate();
        long t1 = System.currentTimeMillis();
        System.out.println("Getting a session took: " + (t1 - t0) + " ms");
        Dataset<Row> df=sparkSession.read().format("org.elasticsearch.spark.sql") //name of the format can be short name
        //or can be full class name
        .option("es.nodes","localhost")
                .option("es.port","9200")
                .option("es.query","?q=*") //here query we want all data
                .option("es.read.field.as.array.include","Inspection_Date") //here we need to convert Inspection_date
        .load("nyc_restaurants") ;//name of dataset
        long t2 = System.currentTimeMillis();
        System.out.println(
                "Init communication and starting to get some results took: "
                        + (t2 - t1) + " ms");
        df.show(10);
        long t3 = System.currentTimeMillis();
        df.printSchema();
        long t4 = System.currentTimeMillis();
        System.out.println("Displaying the schema took: " + (t4 - t3) + " ms");
        System.out.println("The dataframe contains " +
                df.count() + " record(s).");
        long t5 = System.currentTimeMillis();
        System.out.println("Counting the number of records took: " + (t5 - t4)
                + " ms");
        System.out.println("The dataframe is split over " + df.rdd()
                .getPartitions().length + " partition(s).");
        long t6 = System.currentTimeMillis();
        System.out.println("Counting the # of partitions took: " + (t6 - t5)
                + " ms");

    }
}
