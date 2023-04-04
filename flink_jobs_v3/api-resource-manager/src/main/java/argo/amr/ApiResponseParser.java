/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.amr;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricProfile;
import argo.avro.Weight;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Parses a given request's response
 */
public class ApiResponseParser {

    private String tenant;
    private String reportName;
    private String metricID;
    private String aggregationID;
    private String opsID;
    private String threshID;
    private String egroup;

    public ApiResponseParser() {
    }

    public ApiResponseParser(String reportName, String metricID, String aggregationID, String opsID, String threshID, String tenant, String egroup) {
        this.reportName = reportName;
        this.metricID = metricID;
        this.aggregationID = aggregationID;
        this.opsID = opsID;
        this.threshID = threshID;
        this.tenant = tenant;
        this.egroup = egroup;

    }

    public String getReportName() {
        return reportName;
    }

    public void setReportName(String reportName) {
        this.reportName = reportName;
    }

    public String getMetricID() {
        return metricID;
    }

    public void setMetricID(String metricID) {
        this.metricID = metricID;
    }

    public String getAggregationID() {
        return aggregationID;
    }

    public void setAggregationID(String aggregationID) {
        this.aggregationID = aggregationID;
    }

    public String getOpsID() {
        return opsID;
    }

    public void setOpsID(String opsID) {
        this.opsID = opsID;
    }

    public String getThreshID() {
        return threshID;
    }

    public void setThreshID(String threshID) {
        this.threshID = threshID;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getEgroup() {
        return egroup;
    }

    public void setEgroup(String egroup) {
        this.egroup = egroup;
    }

    /**
     * Extract first JSON item from data JSON array in api response
     *
     * @param content JSON content of the full repsonse (status + data)
     * @return First available item in data array as JSON string representation
     */
    public String getJsonData(String content, boolean asArray) {
        
        JsonParser jsonParser = new JsonParser();
        // Grab the first - and only line of json from ops data
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
       
        // Get the data array and the first item
        if(jRoot.get("data")==null) {
            return null;
        }

        if (asArray) {
            return jRoot.get("data").toString();
        }
        
        JsonArray jData = jRoot.get("data").getAsJsonArray();
        if (!jData.iterator().hasNext()) {
            return null;
        }
        JsonElement jItem = jData.get(0);
        return jItem.toString();
    }

    /**
     * Parses the report content to extract the report's name and the various
     * profile IDs
     */
    public void parseReport(String content) {
        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
        JsonArray jProfiles = jRoot.get("profiles").getAsJsonArray();

        JsonObject jInfo = jRoot.get("info").getAsJsonObject();
        this.reportName = jInfo.get("name").getAsString();
        this.tenant = jRoot.get("tenant").getAsString();

        JsonObject topoGroup = jRoot.get("topology_schema").getAsJsonObject().getAsJsonObject("group");
        this.egroup = topoGroup.get("group").getAsJsonObject().get("type").getAsString();

        // for each profile iterate and store it's id in profile manager for later
        // reference
        for (int i = 0; i < jProfiles.size(); i++) {
            JsonObject jProf = jProfiles.get(i).getAsJsonObject();
            String profType = jProf.get("type").getAsString();
            String profID = jProf.get("id").getAsString();
            if (profType.equalsIgnoreCase("metric")) {
                this.metricID = profID;
            } else if (profType.equalsIgnoreCase("aggregation")) {
                this.aggregationID = profID;
            } else if (profType.equalsIgnoreCase("operations")) {
                this.opsID = profID;
            } else if (profType.equalsIgnoreCase("thresholds")) {
                this.threshID = profID;
            }

        }

    }

    public List<String> getListTenants(String content) {
        List<String> results = new ArrayList<String>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jArray = jElement.getAsJsonArray();
        JsonObject jRoot = jArray.get(0).getAsJsonObject();
        JsonArray tenants = jRoot.get("tenants").getAsJsonArray();
        for (int i = 0; i < tenants.size(); i++) {
            String jItem = tenants.get(i).getAsString();
            results.add(jItem);
        }
        return results;
    }

    /**
     * Parses the Downtime content retrieved from argo-web-api and provides a
     * list of Downtime avro objects to be used in the next steps of the
     * pipeline
     */
    public List<Downtime> getListDowntimes(String content) {
        List<Downtime> results = new ArrayList<Downtime>();
        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
        JsonArray jElements = jRoot.get("endpoints").getAsJsonArray();
        for (int i = 0; i < jElements.size(); i++) {
            JsonObject jItem = jElements.get(i).getAsJsonObject();
            String hostname = jItem.get("hostname").getAsString();
            String service = jItem.get("service").getAsString();
            String startTime = jItem.get("start_time").getAsString();
            String endTime = jItem.get("end_time").getAsString();

            Downtime d = new Downtime(hostname, service, startTime, endTime);
            results.add(d);
        }
        return results;

    }

    /**
     * Parses the Topology endpoint content retrieved from argo-web-api and
     * provides a list of GroupEndpoint avro objects to be used in the next
     * steps of the pipeline
     */
    public List<GroupEndpoint> getListGroupEndpoints(String content) {
        List<GroupEndpoint> results = new ArrayList<GroupEndpoint>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();
            String group = jItem.get("group").getAsString();
            String gType = jItem.get("type").getAsString();
            String service = jItem.get("service").getAsString();
            String hostname = jItem.get("hostname").getAsString();
            JsonObject jTags = jItem.get("tags").getAsJsonObject();
            Map<String, String> tags = new HashMap<String, String>();
            for (Map.Entry<String, JsonElement> kv : jTags.entrySet()) {
                tags.put(kv.getKey(), kv.getValue().getAsString());
            }
            GroupEndpoint ge = new GroupEndpoint(gType, group, service, hostname, tags);
            results.add(ge);
        }
        return results;
    }

    /**
     * Parses the Topology Groups content retrieved from argo-web-api and
     * provides a list of GroupGroup avro objects to be used in the next steps
     * of the pipeline
     */
    public List<GroupGroup> getListGroupGroups(String content) {
        List<GroupGroup> results = new ArrayList<GroupGroup>();
        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();
            String group = jItem.get("group").getAsString();
            String gType = jItem.get("type").getAsString();
            String subgroup = jItem.get("subgroup").getAsString();
            JsonObject jTags = jItem.get("tags").getAsJsonObject();
            Map<String, String> tags = new HashMap<String, String>();
            for (Map.Entry<String, JsonElement> kv : jTags.entrySet()) {
                tags.put(kv.getKey(), kv.getValue().getAsString());
            }
            GroupGroup gg = new GroupGroup(gType, group, subgroup, tags);
            results.add(gg);
        }
        return results;

    }

    /**
     * Parses the Weights content retrieved from argo-web-api and provides a
     * list of Weights avro objects to be used in the next steps of the pipeline
     */
    public List<Weight> getListWeights(String content) {
        List<Weight> results = new ArrayList<Weight>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
        String wType = jRoot.get("weight_type").getAsString();
        JsonArray jElements = jRoot.get("groups").getAsJsonArray();
        for (int i = 0; i < jElements.size(); i++) {
            JsonObject jItem = jElements.get(i).getAsJsonObject();
            String group = jItem.get("name").getAsString();
            String weight = jItem.get("value").getAsString();

            Weight w = new Weight(wType, group, weight);
            results.add(w);
        }
        return results;
    }

    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
    public List<MetricProfile> getListMetrics(String content) {
        List<MetricProfile> results = new ArrayList<MetricProfile>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonObject jRoot = jElement.getAsJsonObject();
        String profileName = jRoot.get("name").getAsString();
        JsonArray jElements = jRoot.get("services").getAsJsonArray();
        for (int i = 0; i < jElements.size(); i++) {
            JsonObject jItem = jElements.get(i).getAsJsonObject();
            String service = jItem.get("service").getAsString();
            JsonArray jMetrics = jItem.get("metrics").getAsJsonArray();
            for (int j = 0; j < jMetrics.size(); j++) {
                String metric = jMetrics.get(j).getAsString();

                Map<String, String> tags = new HashMap<String, String>();
                MetricProfile mp = new MetricProfile(profileName, service, metric, tags);
                results.add(mp);
            }

        }
        return results;
    }
    
    
    
        
    /**
     * Compares the metric profile between two dates,and return the ones that are newly introduced
     * @param content
     * @param yesterdayContent
     * @return 
     */
    public List<MetricProfile> getListNewMetrics(String content, String yesterdayContent) {

        List<MetricProfile> results = new ArrayList<MetricProfile>();

        if (yesterdayContent == null) {
            return results;
        }
        results = getListMetrics(content);

        List<MetricProfile> yesterdayResults = new ArrayList<MetricProfile>();
        yesterdayResults = getListMetrics(yesterdayContent);

         results.removeAll(yesterdayResults);
         return results;
    }

}

