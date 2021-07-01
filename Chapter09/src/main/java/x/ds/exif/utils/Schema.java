package x.ds.exif.utils;

import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class Schema {
    private Map<String, SchemaColumn> columns;
    private StructType sparkSchema;
    public void add(SchemaColumn col) {
        this.columns.put(col.getColumnName(), col);
    }
    public StructType getSparkSchema(){
        return this.getSparkSchema();
    }
    public void setSparkSchema(StructType sparkSchema) {
        this.sparkSchema=sparkSchema;
    }
}
