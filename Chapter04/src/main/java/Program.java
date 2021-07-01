import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Program {
    public static void main(String[] args) {
        //Settings
        System.setProperty("hadoop.home.dir","C:/hadoop");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        TransformationAndActionApp app=new TransformationAndActionApp();
        String mode="noop"; //Make sure that you have argument to pass to start().
        if(args.length!=0){
            mode=args[0];
        }
        app.start(mode);
    }
}
