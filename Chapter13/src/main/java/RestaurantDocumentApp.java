import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class RestaurantDocumentApp {
    public void start() {
        SparkSession session = SparkSession.builder()
                .appName("Building a restaurant fact sheet")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> businessDf = session.read().format("csv")
                .option("header", true)
                .load("Chapter13/data/orangecounty_restaurants/businesses.CSV");
        Dataset<Row> inspectionDf = session.read().format("csv")
                .option("header", true)
                .load("Chapter13/data/orangecounty_restaurants/inspections.CSV");

        Dataset<Row> factSheetDf = nestedJoin(
                businessDf,
                inspectionDf,
                "business_id",
                "business_id",
                "inner",
                "inspections"
        );
        factSheetDf.show(3);
        factSheetDf.printSchema();
    }

    private Dataset<Row> nestedJoin(Dataset<Row> leftDf,
                                    Dataset<Row> rightDf,
                                    String leftJoinCol,
                                    String rightJoinCol,
                                    String joinType,
                                    String nestedCol) {

        Dataset<Row> resDf=leftDf.join(rightDf,leftDf.col(leftJoinCol).
                equalTo(rightDf.col(rightJoinCol)),joinType);
        Column[] leftColumns=getColumns(leftDf);
        Column[] allColumns= Arrays.copyOf(leftColumns,leftColumns.length+1);
        allColumns[leftColumns.length]=struct(getColumns(rightDf)).as("Temp_col");
        resDf=resDf.select(allColumns);
        resDf= resDf.groupBy(leftColumns)
                .agg(collect_list("Temp_col").as(nestedCol));
        return resDf;
     }

    private Column[] getColumns(Dataset<Row> df) {
        String[] fieldNames = df.columns();
        Column[] columns=new Column[fieldNames.length];
        int i=0;
        for (String fieldName:fieldNames) {
            columns[i++]=df.col(fieldName);
        }
        return columns;
    }
}
