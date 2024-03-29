/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;

import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 * MetricProfileParser, collects data as described in the json received from web
 * api metric profiles request
 */
public class MetricProfileParser implements Serializable{

    private String id;
    private String date;
    private String name;
    private String description;
    private ArrayList<Services> services=new ArrayList<>();
    private HashMap<String, ArrayList<String>> metricData=new HashMap<>();
    private final String url = "/metric_profiles";
    private JSONObject jsonObject;
    public MetricProfileParser() {
    }

    public MetricProfileParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }
    

    public class Services implements Serializable{

        private String service;
        private ArrayList<String> metrics;

        public Services(String service, ArrayList<String> metrics) {
            this.service = service;
            this.metrics = metrics;
        }

        public String getService() {
            return service;
        }

        public ArrayList<String> getMetrics() {
            return metrics;
        }
    }

    public MetricProfileParser(String apiUri, String key, String proxy, String metricId, String date) throws IOException, ParseException {
        String uri = apiUri + url + "/" + metricId;
        if (date != null) {
            uri = uri + "?date=" + date;
        }
        loadMetricProfile(uri, key, proxy);
    }

    private void loadMetricProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException {
         jsonObject = RequestManager.request(uri, key, proxy);
         readApiRequestResult();
    }
         public void readApiRequestResult(){
             
        JSONArray data = (JSONArray) jsonObject.get("data");
        id = (String) jsonObject.get("id");
        date = (String) jsonObject.get("date");
        name = (String) jsonObject.get("name");
        description = (String) jsonObject.get("description");

        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);

                JSONArray servicesArray = (JSONArray) jsonDataObj.get("services");

                Iterator<Object> iterator = servicesArray.iterator();

                while (iterator.hasNext()) {
                    Object obj = iterator.next();
                    if (obj instanceof JSONObject) {
                        JSONObject servObj = new JSONObject((Map) obj);
                        String serviceName = (String) servObj.get("service");
                        JSONArray metrics = (JSONArray) servObj.get("metrics");
                        Iterator<Object> metrIter = metrics.iterator();
                        ArrayList<String> metricList = new ArrayList<>();

                        while (metrIter.hasNext()) {
                            Object metrObj = metrIter.next();
                            metricList.add(metrObj.toString());
                        }

                        Services service = new Services(serviceName, metricList);
                        services.add(service);
                        metricData.put(serviceName, metricList);
                    }
                }
            }
        }
    }


}
