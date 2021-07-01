package x.ds.exif;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import x.ds.exif.Beans.PhotoMetadata;
import x.ds.exif.utils.ExifUtils;
import x.ds.exif.utils.RecursiveExtensionFilteredLister;
import x.ds.exif.utils.Schema;
import x.ds.exif.utils.SparkBeanUtils;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ExifDirectoryRelation extends BaseRelation implements TableScan, Serializable {
    private Schema schema;
    private static final long serialVersionUID = 4598175080399877334L;

    public void setPhotoLister(RecursiveExtensionFilteredLister photoLister) {
        this.photoLister = photoLister;
    }

    private RecursiveExtensionFilteredLister photoLister;

    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    private SQLContext sqlContext;

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
//            schema = DataTypes.createStructType(new StructField[]
//                    {
//                            DataTypes.createStructField("Fruits",
//                            DataTypes.StringType, true, Metadata.empty())
//                    });
            schema = SparkBeanUtils.getSchemaFromBean(PhotoMetadata.class);
        }
        return schema.getSparkSchema();
    }

    @Override
    public RDD<Row> buildScan() {
        JavaSparkContext context = new JavaSparkContext(sqlContext.sparkContext());
//        List<String> fruits = new ArrayList<>();
//        fruits.add("Mango");
//        fruits.add("Papaya");
//        fruits.add("Apple");
//        fruits.add("Pineapple");
//        fruits.add("Pomegranate");
        List<PhotoMetadata> table = collectData();
        return context.parallelize(table).map(photo ->
                SparkBeanUtils.getRowFromBean(schema, photo)
        ).rdd();
    }

    private List<PhotoMetadata> collectData() {
        List<File> photosToProcess = this.photoLister.getFiles();
        List<PhotoMetadata> list = new ArrayList<>();
        PhotoMetadata photo;
        for (File photoToProcess : photosToProcess) {
            photo = ExifUtils.processFromFilename(
                    photoToProcess.getAbsolutePath());
            list.add(photo);
        }
        return list;
    }
}
