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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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

        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);

        DateTime dt = formatter.parseDateTime(dateStr);

        return dt;
    }

    public static DateTime createDate(String format, Date dateStr, int hour, int min, int sec) throws ParseException {

        //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getDefault());
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTime(dateStr);

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, 0);

        return new DateTime( newCalendar.getTime());
  }

    public static boolean isPreviousDate(String format, Date nowDate, Date firstDate) throws ParseException {
        // String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getDefault());
        cal.setTime(nowDate);

        Calendar calFirst = Calendar.getInstance();
        calFirst.setTime(firstDate);

        if (firstDate.before(nowDate)) {
            return true;
        } else {
            return false;
        }
    }


       public static DateTime createDate(String format, int year, int month, int day, int hour, int min, int sec) throws ParseException {

        // String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getDefault());
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.set(Calendar.YEAR, year);
        newCalendar.set(Calendar.MONTH, month);
        newCalendar.set(Calendar.DAY_OF_MONTH, day);

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, 0);
        return  new DateTime(newCalendar.getTime());
    }
}
