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

/**
 *
 * @author cthermolia
 */
public class Utils {

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

                jsonDataMap.put(hostname + "-" + service, group);
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

    public static HashMap<String, HashMap<String, String>> readOperationProfileJson(String path) {

        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(path));

            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;

            // A JSON array. JSONObject supports java.util.List interface.
            JSONArray dataList = (JSONArray) jsonObject.get("data");

            // An iterator over a collection. Iterator takes the place of Enumeration in the Java Collections Framework.
            // Iterators differ from enumerations in two ways:
            // 1. Iterators allow the caller to remove elements from the underlying collection during the iteration with well-defined semantics.
            // 2. Method names have been improved.
            Iterator<JSONObject> iterator = dataList.iterator();
            HashMap<String, HashMap<String, String>> opTruthTable = new HashMap<>();
            while (iterator.hasNext()) {
                JSONObject dataObject = (JSONObject) iterator.next();
                JSONArray operationList = (JSONArray) dataObject.get("operations");
                Iterator<JSONObject> opIterator = operationList.iterator();
                while (opIterator.hasNext()) {
                    JSONObject operationObject = (JSONObject) opIterator.next();
                    String opName = (String) operationObject.get("name");
                    JSONArray truthtable = (JSONArray) operationObject.get("truth_table");
                    Iterator<JSONObject> truthTableIter = truthtable.iterator();
                    HashMap<String, String> truthTable = new HashMap<>();
                    while (truthTableIter.hasNext()) {
                        JSONObject truthEntry = (JSONObject) truthTableIter.next();
                        String a = (String) truthEntry.get("a");
                        String b = (String) truthEntry.get("b");
                        String x = (String) truthEntry.get("x");

                        truthTable.put(a + "-" + b, x);
                    }
                    opTruthTable.put(opName, truthTable);
                }
            }
            return opTruthTable;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private void buildTruthTable() {

    }
}
