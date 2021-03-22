/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import argo.profiles.TopologyEndpointParser;
import java.util.TimeZone;

/**
 *
 * @author cthermolia
 */
public class Utils {

    static Logger LOG = LoggerFactory.getLogger(Utils.class);
    //   String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public static String getParameterDate(String format, String paramDate) throws ParseException {
        Date date = convertStringtoDate(format, paramDate);

        SimpleDateFormat formatter = new SimpleDateFormat(format);
        String profileDate = formatter.format(date);

        return profileDate;

    }

    public static String convertDateToString(String format, Date date) throws ParseException {

        //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getDefault());
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTime(date);
        newCalendar.set(Calendar.MILLISECOND, 0);
        return sdf.format(newCalendar.getTime());
    }

    public static Date convertStringtoDate(String format, String dateStr) throws ParseException {

        //   String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getDefault());
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(dateStr));
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public static Date createDate(String format, Date dateStr, int hour, int min, int sec) throws ParseException {

        //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getDefault());
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTime(dateStr);

        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, min);
        newCalendar.set(Calendar.SECOND, sec);
        newCalendar.set(Calendar.MILLISECOND, 0);
        return newCalendar.getTime();
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

    public static boolean checkParameters(ParameterTool params, String... vars) {

        for (String var : vars) {

            if (params.get(var) == null) {
                LOG.error("Parameter : " + var + " is required but is missing!\n Program exits!");
                return false;
            }
        }
        return true;

    }

    public static Date createDate(String format, int year, int month, int day, int hour, int min, int sec) throws ParseException {

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

        return newCalendar.getTime();
    }

    public static HashMap<String, String> getEndpoints(ArrayList<TopologyEndpointParser.EndpointGroup> endpointList) throws IOException, org.json.simple.parser.ParseException {

        HashMap<String, String> jsonDataMap = new HashMap<>();

        Iterator<TopologyEndpointParser.EndpointGroup> dataIter = endpointList.iterator();
        while (dataIter.hasNext()) {
            TopologyEndpointParser.EndpointGroup dataobj = dataIter.next();

            String hostname = dataobj.getHostname();
            String service = dataobj.getService();
            String group = dataobj.getGroup();
            jsonDataMap.put(hostname + "-" + service, group);
        }

        return jsonDataMap;
    }

}
