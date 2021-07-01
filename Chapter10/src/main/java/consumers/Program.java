package consumers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeoutException;

public class Program {
    public static void main(String[] args) throws TimeoutException {
        System.setProperty("hadoop.home.dir","c:/hadoop");
        ReadLinesFromFileStreamApp app = new ReadLinesFromFileStreamApp();
        app.start();
    }
}
