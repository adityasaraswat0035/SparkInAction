package x.ds.exif.utils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import x.ds.exif.Beans.PhotoMetadata;
import org.apache.spark.sql.Row;
import x.ds.exif.annotations.SparkColumn;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class SparkBeanUtils {
    private static  int columnIndex=-1;
    public static Schema getSchemaFromBean(Class<?> beanClass) {

        Schema schema = new Schema();
        List<StructField> sfl = new ArrayList<>();
        Method[] methods = beanClass.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            if (!isGetter(method)) {
                continue;
            }
            // The method we are working on is a getter
            String methodName = method.getName();
            SchemaColumn col = new SchemaColumn();
            col.setMethodName(methodName);
            // We have a public method starting with get
            String columnName;
            DataType dataType;
            boolean nullable;
            // Does it have specific annotation?
            SparkColumn sparkColumn = method.getAnnotation(SparkColumn.class);
            if (sparkColumn == null) {
                    columnName="";
                    dataType = getDataTypeFromReturnType(method);
                    nullable=true;
            }
            else{
                columnName = sparkColumn.name();
                switch (sparkColumn.type().toLowerCase()) {
                    case "stringtype":
                    case "string":
                        dataType = DataTypes.StringType;
                        break;
                    case "binarytype":
                    case "binary":
                        dataType = DataTypes.BinaryType;
                        break;
                    case "booleantype":
                    case "boolean":
                        dataType = DataTypes.BooleanType;
                        break;
                    case "datetype":
                    case "date":
                        dataType = DataTypes.DateType;
                        break;
                    case "timestamptype":
                    case "timestamp":
                        dataType = DataTypes.TimestampType;
                        break;
                    case "calendarintervaltype":
                    case "calendarinterval":
                        dataType = DataTypes.CalendarIntervalType;
                        break;
                    case "doubletype":
                    case "double":
                        dataType = DataTypes.DoubleType;
                        break;
                    case "floattype":
                    case "float":
                        dataType = DataTypes.FloatType;
                        break;
                    case "bytetype":
                    case "byte":
                        dataType = DataTypes.ByteType;
                        break;
                    case "integertype":
                    case "integer":
                    case "int":
                        dataType = DataTypes.IntegerType;
                        break;
                    case "longtype":
                    case "long":
                        dataType = DataTypes.LongType;
                        break;
                    case "shorttype":
                    case "short":
                        dataType = DataTypes.ShortType;
                        break;
                    case "nulltype":
                    case "null":
                        dataType = DataTypes.NullType;
                        break;
                    default:
                        dataType = getDataTypeFromReturnType(method);
                }

                nullable = sparkColumn.nullable();
            }
            String finalColumnName = buildColumnName(columnName, methodName);
            sfl.add(DataTypes.createStructField(
                    finalColumnName, dataType, nullable));
            col.setColumnName(finalColumnName);
            schema.add(col);
        }
        StructType sparkSchema = DataTypes.createStructType(sfl);
        schema.setSparkSchema(sparkSchema);
        return schema;
    }

    private static String buildColumnName(String columnName, String methodName) {
        if (columnName.length() > 0) {
            return columnName;
        }
        if (methodName.length() < 4) {
            // Very simplistic
            columnIndex++;
            return "_c" + columnIndex;
        }
        columnName = methodName.substring(3);
        if (columnName.length() == 0) {
            // Very simplistic
            columnIndex++;
            return "_c" + columnIndex;
        }
        return columnName;
    }

    private static DataType getDataTypeFromReturnType(Method method) {
        String typeName = method.getReturnType().getSimpleName().toLowerCase();
        switch (typeName) {
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "long":
                return DataTypes.LongType;
            case "float":
                return DataTypes.FloatType;
            case "boolean":
                return DataTypes.BooleanType;
            case "double":
                return DataTypes.DoubleType;
            case "string":
                return DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "timestamp":
                return DataTypes.TimestampType;
            case "short":
                return DataTypes.ShortType;
            case "object":
            default:
                return DataTypes.BinaryType;
        }
    }

    public static Row getRowFromBean(Schema schema, PhotoMetadata photo) {
        return null;
    }

    private static boolean isGetter(Method method) {
        if (!method.getName().startsWith("get")) {
            return false;
        }
        if (method.getParameterTypes().length != 0) {
            return false;
        }
        if (void.class.equals(method.getReturnType())) {
            return false;
        }
        return true;
    }
}
