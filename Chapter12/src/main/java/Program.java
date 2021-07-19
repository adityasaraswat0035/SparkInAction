import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {
        //Step 0: Set System Properties
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        joinViaSql();

        //transformationViaSql();
    }
    private static void joinViaSql() {
        HigherEdInstitutionPerCountyApp app = new HigherEdInstitutionPerCountyApp();
        app.start();
    }
    private static void transformationViaSql() {
        RecordTransformationApp app = new RecordTransformationApp();
        app.start();
    }
}
