/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package timelines;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Minutes;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utils class to provide functions processing dates
 */
public class Utils {

    static Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static String convertDateToString(String format, DateTime date) throws ParseException {

        //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        DateTimeFormatter dtf = DateTimeFormat.forPattern(format);
        String dateString = date.toString(dtf);
        return dateString;
    }

    public static DateTime convertStringtoDate(String format, String dateStr) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = sdf. parse(dateStr);
        return new DateTime(date.getTime(),DateTimeZone.UTC);

    }

    public static DateTime createDate(String format, Date dateStr, int hour, int min, int sec) throws ParseException {

        //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
     
        newCalendar.setTime(dateStr);

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, 0);
        return new DateTime(newCalendar.getTime(), DateTimeZone.UTC);
    }

    public static boolean isPreviousDate(String format, Date nowDate, Date firstDate) throws ParseException {
        // String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));    
        cal.setTime(nowDate);

        Calendar calFirst = Calendar.getInstance();
        calFirst.setTimeZone(TimeZone.getTimeZone("UTC"));
        calFirst.setTime(firstDate);

        if (firstDate.before(nowDate)) {
            return true;
        } else {
            return false;
        }
    }

    public static DateTime createDate(String format, int year, int month, int day, int hour, int min, int sec) throws ParseException {
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        newCalendar.set(Calendar.YEAR, year);
        newCalendar.set(Calendar.MONTH, month);
        newCalendar.set(Calendar.DAY_OF_MONTH, day);

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, 0);
        
        return new DateTime(newCalendar.getTime(),DateTimeZone.UTC);
    }

    public static DateTime setTime(String format, DateTime dateStr, int hour, int min, int sec, int mill) throws ParseException {

        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        newCalendar.setTime(dateStr.toDate());

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, mill);
        return new DateTime(newCalendar.getTime(), DateTimeZone.UTC);
    }
    public static int calcDayMinutes(DateTime startDay, DateTime endDay) throws ParseException {

        Minutes minutes = Minutes.minutesBetween(startDay, endDay);
        int minutesInt = minutes.getMinutes();
        return minutesInt;
    }

    public static int calcDaySeconds(DateTime startDay, DateTime endDay) throws ParseException {

        startDay = Utils.setTime("yyyy-MM-dd'T'HH:mm:ss'Z'", startDay, 0, 0, 0, 0);
        endDay = Utils.setTime("yyyy-MM-dd'T'HH:mm:ss'Z'", endDay, 23, 59, 59, 59);

        Seconds seconds = Seconds.secondsBetween(startDay, endDay);
        int secondsInt = seconds.getSeconds() + 1;
        return secondsInt;
    }
}