import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        //jsonToFlatten();
         csvToJson();
    }
    private static void csvToJson() {
        RestaurantDocumentApp app=new RestaurantDocumentApp();
        app.start();
    }
    private static void jsonToFlatten() {
        FlattenShipmentDisplayApp app=new FlattenShipmentDisplayApp();
        app.start();
    }
}
