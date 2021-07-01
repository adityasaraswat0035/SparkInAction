import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class PiComputeApp {
    public void start(int slices){
        //Step 1: Define number of throws
        int numberOfThrows=100000*slices;
        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");
        long t0 = System.currentTimeMillis();
        //Step 2: Create Spark Session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark Pi")
                .master("local[*]") //Use all possible threads/core in system
//                .master("spark://un:7077") //when get session from cluster manager or master session resides on  cluster manager
//                .config("spark.executor.memory", "4g")
                .getOrCreate();

        //Step 3: Create Dataframe from List by providing encoder type
        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");
        List<Long> listOfThrows = new ArrayList<Long>(numberOfThrows);
        for (long i=0;i<numberOfThrows;i++){
            listOfThrows.add(i);
        }
        //first dataset created on executor
        Dataset<Row> incrementalDf=sparkSession.createDataset(listOfThrows,Encoders.LONG()).toDF();
        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

        //Step 4: Map the data i.e Throws the Dart
        //This step is added to DAG which sit on ClusterManager
        Dataset<Long> dartsDs = incrementalDf.map(new DartMapper(), Encoders.LONG());
        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");
        //Step 5: Reduce the result to get darts in circle
        //The result of reduce operation is brought back from exeutor to application so firewall communication
        //should be there b/w executor and driver
        long dartsInCircle=dartsDs.reduce(new DartReducer()); //reduce will act as action
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");
        //Estimated value of PI
        System.out.println("Pi is roughly " + 4.0 * dartsInCircle /
                numberOfThrows);

    }
}
