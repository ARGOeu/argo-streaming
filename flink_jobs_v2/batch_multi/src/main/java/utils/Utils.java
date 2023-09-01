/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
        Date date = sdf.parse(dateStr);
        return new DateTime(date.getTime(), DateTimeZone.UTC);

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

    public static boolean checkParameters(ParameterTool params, String... vars) {

        for (String var : vars) {

            if (params.get(var) == null) {
                LOG.error("Parameter : " + var + " is required but is missing!\n Program exits!");
                return false;
            }
        }
        return true;

    }

    public static DateTime createDate(String format, int year, int month, int day, int hour, int min, int sec) throws ParseException {

        // String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        newCalendar.set(Calendar.YEAR, year);

        newCalendar.set(Calendar.MONTH, month);
        newCalendar.set(Calendar.DAY_OF_MONTH, day);

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, 0);

        return new DateTime(newCalendar.getTime(), DateTimeZone.UTC);
    }

    public static DateTime setTime(String format, DateTime dateStr, int hour, int min, int sec, int mill) throws ParseException {

        //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
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

        startDay = Utils.setTime("yyyy-MM-dd'T'HH:mm:ss'Z'", startDay, 0, 0, 0, 0);
        endDay = Utils.setTime("yyyy-MM-dd'T'HH:mm:ss'Z'", endDay, 23, 59, 59, 59);

        Minutes minutes = Minutes.minutesBetween(startDay, endDay);
        int minutesInt = minutes.getMinutes();
        return minutesInt;
    }

     //merges periods of time that are continuous or max 1 min away
    public static ArrayList<String[]> mergeTimestamps(ArrayList<String[]> periods) throws ParseException { 

        ArrayList<String[]> merged = new ArrayList<>();

        boolean toAdd = false;
        String previousStart = null;
        String previousEnd = null;

        for (String[] period : periods) {
             String[] newPeriod = new String[2];
                   
            if (periods.indexOf(period) == 0) {
                previousStart = period[0];
                previousEnd = period[1];
            } else {

                String start = period[0];
                String end = period[1];

                DateTime startDt = convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", start);
                DateTime endDt = convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", previousEnd);

                if (startDt.isAfter(endDt.plusMinutes(1))) { //if the start of current period is after the end of previous period, by more than 1 minute, then save the previous period. else continue
                    newPeriod=new String[2];
                    newPeriod[0] = previousStart;
                    newPeriod[1] = previousEnd;

                    previousStart = start;
                    toAdd=true;
               }
                if(toAdd){
                   merged.add(newPeriod); //add the period to the list
                   toAdd=false; 
                }
                 previousEnd = end; //set the previous end timestamp (used for comparison) to the current's period end

            }
        }
        if (!toAdd) { 
            String[] newPeriod = new String[2];
            newPeriod[0] = previousStart;
            newPeriod[1] = previousEnd;
            merged.add(newPeriod);

        }
        return merged;
    }

}
