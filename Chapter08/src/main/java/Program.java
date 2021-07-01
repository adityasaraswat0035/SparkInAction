import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        //MssqlToDatasetApp();
        //mssqlToDatasetWithOptionsApp();
        //msSQLWithWhereClauseToDatasetApp();
        //msSQLWithJoinToDatasetApp();
        //msSQLToDatasetWithPartitionApp();
        elasticsearchToDatasetApp();
    }

    private static void elasticsearchToDatasetApp() {
        ElasticsearchToDatasetApp app = new ElasticsearchToDatasetApp();
        app.start();

    }

    private static void msSQLToDatasetWithPartitionApp() {
        MSSQLToDatasetWithPartitionApp app = new MSSQLToDatasetWithPartitionApp();
        app.start();
    }
    private static void msSQLWithJoinToDatasetApp() {
        MSSQLWithJoinToDatasetApp app = new MSSQLWithJoinToDatasetApp();
        app.start();
    }
    private static void msSQLWithWhereClauseToDatasetApp() {
        MSSQLWithWhereClauseToDatasetApp app = new MSSQLWithWhereClauseToDatasetApp();
        app.start();
    }

    private static void mssqlToDatasetWithOptionsApp() {
        MssqlToDatasetWithOptionsApp app = new MssqlToDatasetWithOptionsApp();
        app.start();
    }

    private static void MssqlToDatasetApp() {
        MssqlToDatasetApp app = new MssqlToDatasetApp();
        app.start();
    }
}
