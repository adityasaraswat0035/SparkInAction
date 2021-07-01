import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class DartMapper implements MapFunction<Row, Long> {
    private static final long serialVersionUID = 38446L;
    private long counter=0;
    @Override
    public Long call(Row row) throws Exception {
        //we are randomly throwing darts so x and y co-ordiantes
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        /*counter++;
        if (counter % 100000 == 0) {
            System.out.println("" + counter + " darts thrown so far");
        }
        */
        return (x*x+y*y<=1)?1L:0L;

    }
}
