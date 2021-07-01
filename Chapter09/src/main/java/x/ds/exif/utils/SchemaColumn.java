package x.ds.exif.utils;

public class SchemaColumn {
    private String methodName;
    private String columnName;
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    public void setMethodName(String methodName) {
        this.methodName=methodName;
    }

    public String getColumnName() {
        return columnName;
    }
}
