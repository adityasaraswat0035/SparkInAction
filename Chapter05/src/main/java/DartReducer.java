import org.apache.spark.api.java.function.ReduceFunction;

public class DartReducer implements ReduceFunction<Long> {

    @Override
    public Long call(Long x, Long y) throws Exception {
        return  x+y;
    }
}
