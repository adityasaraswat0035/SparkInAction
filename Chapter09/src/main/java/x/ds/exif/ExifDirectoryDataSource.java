package x.ds.exif;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;
import x.ds.exif.utils.K;
import x.ds.exif.utils.RecursiveExtensionFilteredLister;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

public class ExifDirectoryDataSource implements RelationProvider {
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        java.util.Map<String,String> optionsAsJavaMap=mapAsJavaMapConverter(parameters).asJava();
        // Creates a specifif EXIF relation
        ExifDirectoryRelation relation = new ExifDirectoryRelation();
        relation.setSqlContext(sqlContext);
        // Defines the process of acquiring the data through listing files
        RecursiveExtensionFilteredLister photoLister=new RecursiveExtensionFilteredLister();
        for (java.util.Map.Entry<String,String> entry:optionsAsJavaMap.entrySet()){
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            switch (key) {
                case K.PATH:
                    photoLister.setPath(value);
                    break;

                case K.RECURSIVE:
                    if (value.toLowerCase().charAt(0) == 't') {
                        photoLister.setRecursive(true);
                    } else {
                        photoLister.setRecursive(false);
                    }
                    break;

                case K.LIMIT:
                    int limit;
                    try {
                        limit = Integer.valueOf(value);
                    } catch (NumberFormatException e) {
                        limit = -1;
                    }
                    photoLister.setLimit(limit);
                    break;

                case K.EXTENSIONS:
                    String[] extensions = value.split(",");
                    for (int i = 0; i < extensions.length; i++) {
                        photoLister.addExtension(extensions[i]);
                    }
                    break;
                default:
                    break;
            }
        }
        relation.setPhotoLister(photoLister);
        return relation;
    }
}
