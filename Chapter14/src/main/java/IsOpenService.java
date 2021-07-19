import scala.collection.script.Start;

import java.sql.Timestamp;
import java.util.Calendar;

public class IsOpenService {
    public static Boolean isOpen(String hoursMon,
                                 String hoursTue, String hoursWed, String hoursThu,
                                 String hoursFri, String hoursSat, String hoursSun, Timestamp dateTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(dateTime.getTime());
        int day=cal.get(Calendar.DAY_OF_WEEK);
        String hours="closed";
        switch (day){
            case Calendar.MONDAY:
                hours=hoursMon;
            case Calendar.TUESDAY:
                hours=hoursMon;
            case Calendar.WEDNESDAY:
                hours=hoursMon;
            case Calendar.THURSDAY:
                hours=hoursMon;
            case Calendar.FRIDAY:
                hours=hoursMon;
            case Calendar.SATURDAY:
                hours=hoursMon;
            case Calendar.SUNDAY:
                hours=hoursMon;
        }
        if(hours.compareToIgnoreCase("closed")==0){
            return false;
        }
        int event=cal.get(Calendar.HOUR_OF_DAY)*3600
                + cal.get(Calendar.MINUTE) * 60
                + cal.get(Calendar.SECOND);
        String[] ranges = hours.split(" and ");
        for (int i=0;i<ranges.length;i++){
            String[] operningHours = ranges[i].split("-");
            int start=Integer.valueOf(operningHours[0].substring(0, 2)) * 3600 +
                    Integer.valueOf(operningHours[0].substring(3, 5)) * 60;
            int end=Integer.valueOf(operningHours[1].substring(0, 2)) * 3600 +
                    Integer.valueOf(operningHours[1].substring(3, 5)) * 60;
            if(event>= start && event<=end){
                return true;
            }
        }
        return  false;
    }
}
