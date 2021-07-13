package argo.commons.profiles;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import argo.commons.requests.RequestManager;

/**
 *
 * @author cthermolia
 *
 * MetricProfileParser, collects data as described in the json received from web
 * api metric profiles request
 */
public class MetricProfileParser implements Serializable {

    private String id;
    private String date;
    private String name;
    private String description;
    private ArrayList<Services> services = new ArrayList<>();
    private HashMap<String, ArrayList<String>> metricData = new HashMap<>();
    private final String url = "/metric_profiles";
    private JSONObject jsonObject;

    public MetricProfileParser() {
    }

    public MetricProfileParser(String apiUri, String key, String proxy, String metricId, String date) throws IOException, ParseException {
        String uri = apiUri + url + "/" + metricId;
        if (date != null) {
            uri = uri + "?date=" + date;
        }
        loadMetricProfile(uri, key, proxy);
    }

    public MetricProfileParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }

    private void loadMetricProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException {
        jsonObject = RequestManager.request(uri, key, proxy);
        readApiRequestResult();

    }

    public void readApiRequestResult() {
        JSONArray data = (JSONArray) jsonObject.get("data");

        JSONObject dataObject = (JSONObject) data.get(0);

        id = (String) dataObject.get("id");
        date = (String) dataObject.get("date");
        name = (String) dataObject.get("name");
        description = (String) dataObject.get("description");

        //   JSONArray jsonArray=dataObject.get("services");
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

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public class Services implements Serializable {

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

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 53 * hash + Objects.hashCode(this.service);
            hash = 53 * hash + Objects.hashCode(this.metrics);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Services other = (Services) obj;
            if (!Objects.equals(this.service, other.service)) {
                return false;
            }
            if (!Objects.equals(this.metrics, other.metrics)) {
                return false;
            }
            return true;
        }

    }

    public boolean containsMetric(String service, String metric) {

        if (metricData.get(service) != null && metricData.get(service).contains(metric)) {
            return true;
        }
        return false;
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this.id);
        hash = 19 * hash + Objects.hashCode(this.date);
        hash = 19 * hash + Objects.hashCode(this.name);
        hash = 19 * hash + Objects.hashCode(this.description);
        hash = 19 * hash + Objects.hashCode(this.services);
        hash = 19 * hash + Objects.hashCode(this.metricData);
        hash = 19 * hash + Objects.hashCode(this.url);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MetricProfileParser other = (MetricProfileParser) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.date, other.date)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.description, other.description)) {
            return false;
        }
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Objects.equals(this.services, other.services)) {
            return false;
        }
        if (!Objects.equals(this.metricData, other.metricData)) {
            return false;
        }
        return true;
    }

}
