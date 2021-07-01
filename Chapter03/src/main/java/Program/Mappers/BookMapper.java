package Program.Mappers;
import Program.Pojo.Book;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.MapFunction;

import java.text.SimpleDateFormat;

public class BookMapper implements MapFunction<Row, Book> {
    private static final long serialVersionUID = -2L;
    @Override
    public Book call(Row row) throws Exception {
        Book book=new Book();
        book.setId(row.getAs("id"));
        book.setAuthorId(row.getAs("authorId"));
        book.setLink(row.getAs("link"));
        book.setTitle(row.getAs("title"));
        // date case
        String dateAsString = row.getAs("releaseDate");
        if(dateAsString!=null)
        {
            SimpleDateFormat dateFormat=new SimpleDateFormat("M/d/yy");
            book.setReleaseDate(dateFormat.parse(dateAsString));
        }
        return  book;
    }
}
