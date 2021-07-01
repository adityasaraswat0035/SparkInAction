import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        PhotoMetadataIngestionApp app=new PhotoMetadataIngestionApp();
        app.start();

    }
}
