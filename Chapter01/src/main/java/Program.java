import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {    //-> Entry point of Application called Driver Code

        // Code Needed only in case of windows
        System.setProperty("hadoop.home.dir","C:/hadoop");
        //turn off the logging of apache or can provide log4j.properties
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        CsvToDataframeApp app = new CsvToDataframeApp();
        app.start();
    }
}
