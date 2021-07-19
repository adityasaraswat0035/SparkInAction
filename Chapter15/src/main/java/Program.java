import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) throws Exception {
        //Step 0: Setup
        System.setProperty("hadoop.home.dir","C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        //orderStatisticsApp();
        //newYorkSchoolStatisticsApp();
        pointsPerOrderApp();
    }

    private static  void pointsPerOrderApp(){
        PointsPerOrderApp app=new PointsPerOrderApp();
        app.start();
    }

    private static void orderStatisticsApp() {
        OrderStatisticsApp app=new OrderStatisticsApp();
        app.start();
    }

    private static void newYorkSchoolStatisticsApp() throws Exception {
       try( NewYorkSchoolStatisticsApp app=new NewYorkSchoolStatisticsApp();){
           app.start();
       }
    }
}
