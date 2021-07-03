import org.apache.spark.sql.AnalysisException;

public class Program {
    public static void main(String[] args) throws AnalysisException {
        System.setProperty("hadoop.home.dir","C:/hadoop");
        //SimpleSelectApp app=new SimpleSelectApp();
        //SimpleSelectGlobalViewApp app=new SimpleSelectGlobalViewApp();
        //SqlAndApiApp app=new SqlAndApiApp();
        DeleteApp app=new DeleteApp();
        app.start();
    }
}
