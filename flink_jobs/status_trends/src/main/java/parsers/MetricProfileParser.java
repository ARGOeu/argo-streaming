/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parsers;

import argo.utils.RequestManager;
import com.google.common.util.concurrent.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class MetricProfileParser {

    private String id;
    private String date;
    private String name;
    private String description;
    private ArrayList<Services> services;
    private HashMap<String, ArrayList<String>> metricData;

    public class Services {

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

    public MetricProfileParser(String baseUri, String key, String proxy, String metricId, String date) throws IOException, ParseException {

        loadMetricProfile(baseUri, key, proxy, metricId, date);
    }

    private void loadMetricProfile(String baseUri, String key, String proxy, String metricId, String dateStr) throws IOException, org.json.simple.parser.ParseException {
        JSONObject jsonObject = RequestManager.getMetricProfileRequest(baseUri, key, proxy, metricId, dateStr);

        services = new ArrayList<>();
        metricData = new HashMap<>();
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

    public String getId() {
        return id;
    }

    public String getDate() {
        return date;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public ArrayList<Services> getServices() {
        return services;
    }

    public HashMap<String, ArrayList<String>> getMetricData() {
        return metricData;
    }

}
