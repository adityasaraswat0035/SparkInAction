package x.ds.exif;

import org.apache.spark.sql.sources.DataSourceRegister;

public class ExifDirectoryDatasourceShortNameAdvertiser extends ExifDirectoryDataSource
        implements DataSourceRegister {
    @Override
    public String shortName() {
        return "exif";
    }

}
