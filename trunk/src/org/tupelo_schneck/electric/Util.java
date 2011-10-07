package org.tupelo_schneck.electric;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;

public class Util {
    public static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    private static int parseInt(String s,int def) {
        if(s==null) return def;
        return Integer.parseInt(s);
    }

    private static final Pattern dateTimePattern = Pattern.compile("(\\d\\d\\d\\d)(?:-?+(\\d\\d?+)(?:-?+(\\d\\d?+)(?:[T\\s]++(\\d\\d??)(?>:?+(\\d\\d)(?>:?+(\\d\\d)(?>[.,]\\d*+)?+)?+)?+)?)?)?(Z|[+-](\\d\\d):?+(\\d\\d)?+)?+");

    public static int timestampFromUserInput(String aInput, boolean isEnd, TimeZone aTimeZone) {
        String input = aInput.trim();
        Matcher matcher = dateTimePattern.matcher(input);
        if(!matcher.matches()) {
            return Integer.parseInt(input);
        }
        
        int year = parseInt(matcher.group(1),1);
        int month = parseInt(matcher.group(2),1);
        int day = parseInt(matcher.group(3),1);
        int hour = parseInt(matcher.group(4),0);
        int minute = parseInt(matcher.group(5),0);
        int second = parseInt(matcher.group(6),0);
        
        TimeZone thisTimeZone = aTimeZone;
        int timeZoneHours = 0;
        int timeZoneMinutes = 0;
        if(matcher.group(7)!=null) {
            thisTimeZone = GMT;
            char start = matcher.group(7).charAt(0);
            if(start=='+' || start=='-') {
                timeZoneHours = parseInt(matcher.group(8),0);
                timeZoneMinutes = parseInt(matcher.group(9),0);
                if(start=='-') {
                    timeZoneHours = -timeZoneHours;
                    timeZoneMinutes = -timeZoneMinutes;
                }
            }
        }
        
        GregorianCalendar calendar = new GregorianCalendar(thisTimeZone);
        calendar.set(year,month-1,day,hour,minute,second);
        if(isEnd) {
            boolean setting = matcher.group(2)==null;
            if(setting) calendar.set(GregorianCalendar.MONTH,calendar.getActualMaximum(GregorianCalendar.MONTH)); 
            setting = setting || matcher.group(3)==null;
            if(setting) calendar.set(GregorianCalendar.DAY_OF_MONTH,calendar.getActualMaximum(GregorianCalendar.DAY_OF_MONTH)); 
            setting = setting || matcher.group(4)==null;
            if(setting) calendar.set(GregorianCalendar.HOUR_OF_DAY,calendar.getActualMaximum(GregorianCalendar.HOUR_OF_DAY)); 
            setting = setting || matcher.group(5)==null;
            if(setting) calendar.set(GregorianCalendar.MINUTE,calendar.getActualMaximum(GregorianCalendar.MINUTE)); 
            setting = setting || matcher.group(6)==null;
            if(setting) calendar.set(GregorianCalendar.SECOND,calendar.getActualMaximum(GregorianCalendar.SECOND)); 
        }
        calendar.add(GregorianCalendar.HOUR_OF_DAY, -timeZoneHours);
        calendar.add(GregorianCalendar.MINUTE, -timeZoneMinutes);
        
        long time = calendar.getTimeInMillis() / 1000;
        if(time < 0) return 0;
        if(time > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int)time;
    }

    public static boolean inDSTOverlap(TimeZone timeZone, int timestamp) {
        int offsetNow = timeZone.getOffset(1000L*timestamp);
        int offsetHourAgo = timeZone.getOffset(1000L*(timestamp-3600));
        return offsetHourAgo > offsetNow;
    }

}
