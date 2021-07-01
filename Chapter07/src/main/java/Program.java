import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        //complexCsvToDataframeApp();
        //complexCsvToDataframeWithSchemaApp();
        //jsonLinesToDataframeApp();
        // multilineJsonToDataframeApp();
        //xmlToDataframeApp();
        //textToDataframeApp();
        //avroToDataframeApp();
        //orcToDataframeApp();
        parquetToDataframeApp();
    }

    private static void parquetToDataframeApp() {
        ParquetToDataframeApp app = new ParquetToDataframeApp();
        app.start();

    }

    private static void orcToDataframeApp() {
        OrcToDataframeApp app = new OrcToDataframeApp();
        app.start();

    }
    private static void avroToDataframeApp() {
        AvroToDataframeApp app = new AvroToDataframeApp();
        app.start();

    }
    private static void textToDataframeApp() {
        TextToDataframeApp app = new TextToDataframeApp();
        app.start();

    }

    private static void xmlToDataframeApp() {
        XmlToDataframeApp app = new XmlToDataframeApp();
        app.start();

    }

    private static void multilineJsonToDataframeApp() {
        MultilineJsonToDataframeApp app = new MultilineJsonToDataframeApp();
        app.start();
    }

    private static void jsonLinesToDataframeApp() {
        JsonLinesToDataframeApp app = new JsonLinesToDataframeApp();
        app.start();
    }

    private static void complexCsvToDataframeWithSchemaApp() {
        ComplexCsvToDataframeWithSchemaApp app = new ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    private static void complexCsvToDataframeApp() {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app.start();
    }
}
