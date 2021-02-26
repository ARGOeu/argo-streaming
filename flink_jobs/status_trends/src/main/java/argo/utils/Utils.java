/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.utils;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
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


    
     public static HashMap<String, ArrayList<String>> readMetricDataJson(String baseUri, String metricProfileUUID, String key) throws IOException, org.json.simple.parser.ParseException {
        JSONObject jsonObject = RequestManager.getMetricProfileRequest(baseUri, metricProfileUUID, key);
        HashMap<String, ArrayList<String>> jsonDataMap = new HashMap<String, ArrayList<String>>();

        JSONArray data = (JSONArray) jsonObject.get("data");

        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);

                JSONArray services = (JSONArray) jsonDataObj.get("services");

                Iterator<Object> iterator = services.iterator();

                while (iterator.hasNext()) {
                    Object obj = iterator.next();
                    if (obj instanceof JSONObject) {
                        JSONObject servObj = new JSONObject((Map) obj);
                        String service = (String) servObj.get("service");
                        JSONArray metrics = (JSONArray) servObj.get("metrics");
                        Iterator<Object> metrIter = metrics.iterator();
                        ArrayList<String> metricList = new ArrayList<>();

                        while (metrIter.hasNext()) {
                            Object metrObj = metrIter.next();
                            metricList.add(metrObj.toString());
                        }
                        jsonDataMap.put(service, metricList);
                    }
                }
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

            JSONArray dataList = (JSONArray) jsonObject.get("data");

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

    public static boolean checkParameters(ParameterTool params, String... vars) {

        for (String var : vars) {

            if (params.get(var) == null) {
                LOG.error("Parameter : "+var + " is required but is missing!\n Program exits!");
                return false;
            }
        }
        return true;

    }

    public static HashMap<String, String> readGroupEndpointJson(String baseUri, String key) throws IOException, org.json.simple.parser.ParseException {
        JSONObject jsonObject = RequestManager.getTopologyEndpointRequest(baseUri, key);
        HashMap<String, String> jsonDataMap = new HashMap<>();

        JSONArray data = (JSONArray) jsonObject.get("data");

        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);
                String hostname = (String) jsonDataObj.get("hostname");
                String service = (String) jsonDataObj.get("service");
                String group = (String) jsonDataObj.get("group");
                jsonDataMap.put(hostname + "-" + service, group);
            }
        }

        return jsonDataMap;
    }


}
