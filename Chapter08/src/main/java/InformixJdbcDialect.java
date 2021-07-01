import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

import java.util.Locale;

public class InformixJdbcDialect extends JdbcDialect {
    //JdbcDialect is serializable, hence a unique ID for this class.
    private static final long serialVersionUID = -672901;

    @Override
    public boolean canHandle(String url) {
        //this filter method will allow spark to know which driver to use in which context.
        return url.startsWith("jdbc:informix-sqli");
    }

    //this method convert a sqltype to a spark type
    @Override
    public Option<DataType> getCatalystType(int sqlType,
                                            String typeName, int size, MetadataBuilder md) {
        if (typeName.toLowerCase().compareTo("serial") == 0) {
            return Option.apply(DataTypes.IntegerType);
        }
        if (typeName.toLowerCase().compareTo("calendar") == 0) {
             return Option.apply(DataTypes.BinaryType);
        }
        return Option.empty();
    }

}
