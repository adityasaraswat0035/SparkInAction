package x.ds.exif.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

    /**
     * The name of the column can be overriden.
     */
    String name() default "";

    /**
     * Forces the data type of the column
     */
    String type() default "";

    /**
     * Forces the required/nullable property
     */
    boolean nullable() default true;
}
