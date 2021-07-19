import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

public class PointAttributionUdaf extends Aggregator<Integer,Integer,Integer> {
    private static final long serialVersionUID = -66830400L;
    public static final int MAX_POINT_PER_ORDER = 3;

    @Override
    public Integer zero() {
        return 0;
    }

    @Override
    public Integer reduce(Integer b, Integer a) {
        int bufferValue=b;
        int inputValue=a;
        int outputValue=0;
        if(a<MAX_POINT_PER_ORDER){
            outputValue=a;
        }
        else{
            outputValue=MAX_POINT_PER_ORDER;
        }
        outputValue+=bufferValue;
        return outputValue;
    }
    @Override
    public Integer merge(Integer b1, Integer b2) {
        int buffer1=b1;
        int buffer2=b2;
        return buffer1+buffer2;
    }

    @Override
    public Integer finish(Integer reduction) {
        int bufferResult=reduction;
        return bufferResult;
    }

    @Override
    public Encoder<Integer> bufferEncoder() {
        return Encoders.INT();
    }

    @Override
    public Encoder<Integer> outputEncoder() {
        return Encoders.INT();
    }
}
