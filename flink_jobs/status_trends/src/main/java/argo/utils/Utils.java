/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.utils;

import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 */
public class Utils {
    static Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static  HashMap<String, String> opAndTruthTable() {
        
        HashMap<String, String> truthTable = new HashMap<String, String>();
        truthTable.put("OK-OK", "OK");
        truthTable.put("OK-WARNING", "WARNING");
        truthTable.put("OK-UNKNOWN", "UNKNOWN");
        truthTable.put("OK-MISSING", "MISSING");
        truthTable.put("OK-CRITICAL", "CRITICAL");
        truthTable.put("OK-DOWNTIME", "DOWNTIME");

        truthTable.put("WARNING-WARNING", "WARNING");
        truthTable.put("WARNING-UNKNOWN", "UNKNOWN");
        truthTable.put("WARNING-MISSING", "MISSING");
        truthTable.put("WARNING-CRITICAL", "CRITICAL");
        truthTable.put("WARNING-DOWNTIME", "DOWNTIME");

        truthTable.put("UNKNOWN-UNKNOWN", "UNKNOWN");
        truthTable.put("UNKNOWN-MISSING", "MISSING");
        truthTable.put("UNKNOWN-CRITICAL", "CRITICAL");
        truthTable.put("UNKNOWN-DOWNTIME", "DOWNTIME");

        truthTable.put("MISSING-MISSING", "MISSING");
        truthTable.put("MISSING-CRITICAL", "CRITICAL");
        truthTable.put("MISSING-DOWNTIME", "DOWNTIME");

        truthTable.put("CRITICAL-CRITICAL", "CRITICAL");

        truthTable.put("CRITICAL-DOWNTIME", "CRITICAL");
        truthTable.put("DOWNTIME-DOWNTIME", "DOWNTIME");
        
        return truthTable;

    }

    public static String convertDateToString(Date date) throws ParseException {

        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.setTime(date);

        return sdf.format(newCalendar.getTime());
    }
   
    public static Date convertStringtoDate(String dateStr) throws ParseException {

        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(dateStr));

        return cal.getTime();
    }

    public static String createDate(String dateStr, int hour, int min, int sec) throws ParseException {

        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.set(2021, 0, 15, hour, min, sec);

        return sdf.format(newCalendar.getTime());
    }

    public static boolean isPreviousDate(String nowDate, String firstDate) throws ParseException {
        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        cal.setTime(sdf.parse(nowDate));

        Calendar calFirst = Calendar.getInstance();
        calFirst.setTime(sdf.parse(firstDate));

        if (calFirst.getTime().before(cal.getTime())) {
            return true;
        } else {
            return false;
        }
    }

    public static HashMap<String, String> readGroupEndpointJson(String path) {

        JSONParser parser = new JSONParser();
        try {
            JSONArray jsonArray = new JSONArray();
            HashMap<String, String> jsonDataMap = new HashMap<String, String>();
            jsonArray.addAll((List) parser.parse(new FileReader(path)));
            jsonDataMap = convertGroupEndpointsJson(jsonArray);
            return jsonDataMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static HashMap<String, ArrayList<String>> readMetricDataJson(String path) {

        JSONParser parser = new JSONParser();
        try {
            JSONArray jsonArray = new JSONArray();
            HashMap<String, ArrayList<String>> jsonDataMap = new HashMap<String, ArrayList<String>>();
            jsonArray.addAll((List) parser.parse(new FileReader(path)));
            jsonDataMap = convertMetricDataJson(jsonArray);
            return jsonDataMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static HashMap<String, String> convertGroupEndpointsJson(JSONArray jsonArray) {
        HashMap<String, String> jsonDataMap = new HashMap<>();
        Iterator<Object> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            Object obj = iterator.next();
            if (obj instanceof JSONObject) {
                JSONObject jsonObject = new JSONObject((Map) obj);
               String hostname = (String) jsonObject.get("hostname");
               String service = (String) jsonObject.get("service");
               String group = (String) jsonObject.get("group");

                jsonDataMap.put(hostname+"-"+service, group);
            }
        }
        return jsonDataMap;

    }

    public static HashMap<String, ArrayList<String>> convertMetricDataJson(JSONArray jsonArray) {
        HashMap<String, ArrayList<String>> jsonDataMap = new HashMap<>();
        Iterator<Object> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            Object obj = iterator.next();
            if (obj instanceof JSONObject) {
                JSONObject jsonObject = new JSONObject((Map) obj);
                String service = (String) jsonObject.get("service");
                String metric = (String) jsonObject.get("metric");
                ArrayList<String> metricList;
                if (jsonDataMap.get(service) != null) {
                    metricList = jsonDataMap.get(service);
                } else {
                    metricList = new ArrayList<String>();
                }
                metricList.add(metric);
                jsonDataMap.put(service, metricList);
            }
        }
        return jsonDataMap;
    }

}
