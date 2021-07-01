package consumers;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class AgeChecker extends ForeachWriter<Row> implements Serializable {
    private static final long serialVersionUID = 8383715100587612498L;
    private int streamId = 0;

    public AgeChecker(int streamId) {
        this.streamId = streamId;
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        //Implement this method when you open
        //your writer; not applicable here.
        return false;
    }

    @Override
    public void process(Row row) {
        //Method to process a row
        int age = row.getInt(3);
        if (age < 13) {
            System.out.println(String.format("On stream #{}: {} is a kid, they are {} yrs old.",
                    streamId,
                    row.getString(0),
                    age));
        } else if (age > 12 && age < 20) {
            System.out.println(String.format("On stream #{}: {} is a teen, they are {} yrs old.",
                    streamId,
                    row.getString(0),
                    age));
        } else if (age > 64) {
            System.out.println(String.format("On stream #{}: {} is a senior, they are {} yrs old.",
                    streamId,
                    row.getString(0),
                    age));
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        //Implement this method when you close
        //your writer; not applicable here.

    }
}
