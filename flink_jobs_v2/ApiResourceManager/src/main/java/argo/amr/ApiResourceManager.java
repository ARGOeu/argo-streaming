package argo.amr;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricProfile;
import argo.avro.Weight;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/**
 * APIResourceManager class fetches remote argo-web-api resources such as report
 * configuration, profiles, topology, weights in JSON format
 */
public class ApiResourceManager {

    private EnumMap<ApiResource, String> data = new EnumMap<>(ApiResource.class);

    private String endpoint;
    private String token;
    private String reportID;
    private String date;
    private String proxy;

    private String metricID;
    private String aggregationID;
    private String opsID;
    private String threshID;
    private String reportName;
    private String weightsID;
    private RequestManager requestManager;
    private ApiResponseParser apiResponseParser;
    //private boolean verify;
    //private int timeoutSec;

    public ApiResourceManager(String endpoint, String token) {
        this.endpoint = endpoint;
        this.token = token;
        this.metricID = "";
        this.aggregationID = "";
        this.opsID = "";
        this.threshID = "";
        this.reportName = "";
        this.reportID = "";
        this.date = "";
        this.weightsID = "";
        this.requestManager = new RequestManager("", this.token);
        this.apiResponseParser = new ApiResponseParser(this.reportName, this.metricID, this.aggregationID, this.opsID, this.threshID);
    }

    public EnumMap<ApiResource, String> getData() {
        return data;
    }

    public void setData(EnumMap<ApiResource, String> data) {
        this.data = data;
    }

    public ApiResponseParser getApiResponseParser() {
        return apiResponseParser;
    }

    public void setApiResponseParser(ApiResponseParser apiResponseParser) {
        this.apiResponseParser = apiResponseParser;
    }

    public String getThreshID() {
        return threshID;
    }

    public void setThreshID(String threshID) {
        this.threshID = threshID;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getReportID() {
        return reportID;
    }

    public void setReportID(String reportID) {
        this.reportID = reportID;
    }

    public String getReportName() {
        return this.reportName;
    }

    public String getOpsID() {
        return this.opsID;
    }

    public String getAggregationID() {
        return this.aggregationID;
    }

    public String getMetricID() {
        return this.metricID;
    }

    public String getThresholdsID() {
        return this.threshID;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public RequestManager getRequestManager() {
        return requestManager;
    }

    public void setRequestManager(RequestManager requestManager) {
        this.requestManager = requestManager;
    }

//    public ApiResponseParser getApiResponseParser() {
//        return apiResponseParser;
//    }
//
//    public void setApiResponseParser(ApiResponseParser apiResponseParser) {
//        this.apiResponseParser = apiResponseParser;
//    }
    public String getWeightsID() {
        return weightsID;
    }

    public void setWeightsID(String weightsID) {
        this.weightsID = weightsID;
    }

    /**
     * Retrieves the remote report configuration based on reportID main class
     * attribute and stores the content in the enum map
     */
    public void getRemoteConfig() {
        String path = "https://%s/api/v2/reports/%s";
        String fullURL = String.format(path, this.endpoint, this.reportID);
        String content = this.requestManager.getResource(fullURL);
        this.data.put(ApiResource.CONFIG, this.apiResponseParser.getJsonData(content, false));

    }

    /**
     * Retrieves the metric profile content based on the metric_id attribute and
     * stores it to the enum map
     */
    public void getRemoteMetric() {

        String path = "https://%s/api/v2/metric_profiles/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.metricID, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.METRIC, this.apiResponseParser.getJsonData(content, false));

    }

    /**
     * Retrieves the aggregation profile content based on the aggreagation_id
     * attribute and stores it to the enum map
     */
    public void getRemoteAggregation() {

        String path = "https://%s/api/v2/aggregation_profiles/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.aggregationID, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.AGGREGATION, this.apiResponseParser.getJsonData(content, false));

    }

    /**
     * Retrieves the ops profile content based on the ops_id attribute and
     * stores it to the enum map
     */
    public void getRemoteOps() {

        String path = "https://%s/api/v2/operations_profiles/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.opsID, this.date);

        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.OPS, this.apiResponseParser.getJsonData(content, false));

    }

    /**
     * Retrieves the thresholds profile content based on the thresh_id attribute
     * and stores it to the enum map
     */
    public void getRemoteThresholds() {

        String path = "https://%s/api/v2/thresholds_profiles/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.threshID, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.THRESHOLDS, this.apiResponseParser.getJsonData(content, false));

    }

    /**
     * Retrieves the topology endpoint content and stores it to the enum map
     */
    public void getRemoteTopoEndpoints() {
        String path = "https://%s/api/v2/topology/endpoints/by_report/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.reportName, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.TOPOENDPOINTS, this.apiResponseParser.getJsonData(content, true));

    }

    /**
     * Retrieves the topology groups content and stores it to the enum map
     */
    public void getRemoteTopoGroups() {
        String path = "https://%s/api/v2/topology/groups/by_report/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.reportName, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.TOPOGROUPS, this.apiResponseParser.getJsonData(content, true));

    }

    /**
     * Retrieves the weights content and stores it to the enum map
     */
    public void getRemoteWeights() {
        String path = "https://%s/api/v2/weights/%s?date=%s";
        String fullURL = String.format(path, this.endpoint, this.weightsID, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.WEIGHTS, this.apiResponseParser.getJsonData(content, false));

    }

    /**
     * Retrieves the downtimes content and stores it to the enum map
     */
    public void getRemoteDowntimes() {
        String path = "https://%s/api/v2/downtimes?date=%s";
        String fullURL = String.format(path, this.endpoint, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.DOWNTIMES, this.apiResponseParser.getJsonData(content, false));

    }

    public void getRemoteRecomputations() {
        String path = "https://%s/api/v2/recomputations?date=%s";
        String fullURL = String.format(path, this.endpoint, this.date);
        String content = this.requestManager.getResource(fullURL);

        this.data.put(ApiResource.RECOMPUTATIONS, this.apiResponseParser.getJsonData(content, true));

    }

    /**
     * Returns local resource (after has been retrieved) content based on
     * resource type
     *
     * @param res
     * @return The extracted items JSON value as string
     */
    public String getResourceJSON(ApiResource res) {
        return this.data.get(res);
    }

    /**
     * Parses the report content to extract the report's name and the various
     * profile IDs
     */
    public void parseReport() {

        // check if report configuration has been retrieved
        if (!this.data.containsKey(ApiResource.CONFIG)) {
            return;
        }

        String content = this.data.get(ApiResource.CONFIG);
        this.apiResponseParser.parseReport(content);
        this.metricID = this.apiResponseParser.getMetricID();
        this.aggregationID = this.apiResponseParser.getAggregationID();
        this.opsID = this.apiResponseParser.getOpsID();
        this.threshID = this.apiResponseParser.getThreshID();
        this.reportName = this.apiResponseParser.getReportName();
    }

    /**
     * Parses the Downtime content retrieved from argo-web-api and provides a
     * list of Downtime avro objects to be used in the next steps of the
     * pipeline
     */
    public Downtime[] getListDowntimes() {

        List<Downtime> results = new ArrayList<Downtime>();
        if (!this.data.containsKey(ApiResource.DOWNTIMES)) {
            Downtime[] rArr = new Downtime[results.size()];
            rArr = results.toArray(rArr);
        }

        String content = this.data.get(ApiResource.DOWNTIMES);
        results = this.apiResponseParser.getListDowntimes(content);
        Downtime[] rArr = new Downtime[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * Parses the Topology endpoint content retrieved from argo-web-api and
     * provides a list of GroupEndpoint avro objects to be used in the next
     * steps of the pipeline
     */
    public GroupEndpoint[] getListGroupEndpoints() {
        List<GroupEndpoint> results = new ArrayList<GroupEndpoint>();
        if (!this.data.containsKey(ApiResource.TOPOENDPOINTS)) {
            GroupEndpoint[] rArr = new GroupEndpoint[results.size()];
            rArr = results.toArray(rArr);
            return rArr;
        }

        String content = this.data.get(ApiResource.TOPOENDPOINTS);
        results = this.apiResponseParser.getListGroupEndpoints(content);
        GroupEndpoint[] rArr = new GroupEndpoint[results.size()];
        rArr = results.toArray(rArr);
        return rArr;

    }

    /**
     * Parses the Topology Groups content retrieved from argo-web-api and
     * provides a list of GroupGroup avro objects to be used in the next steps
     * of the pipeline
     */
    public GroupGroup[] getListGroupGroups() {

        List<GroupGroup> results = new ArrayList<GroupGroup>();
        if (!this.data.containsKey(ApiResource.TOPOGROUPS)) {
            GroupGroup[] rArr = new GroupGroup[results.size()];
            rArr = results.toArray(rArr);
            return rArr;
        }
        String content = this.data.get(ApiResource.TOPOGROUPS);
        results = this.apiResponseParser.getListGroupGroups(content);
        GroupGroup[] rArr = new GroupGroup[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * Parses the Weights content retrieved from argo-web-api and provides a
     * list of Weights avro objects to be used in the next steps of the pipeline
     */
    public Weight[] getListWeights() {
        List<Weight> results = new ArrayList<Weight>();
        if (!this.data.containsKey(ApiResource.WEIGHTS)) {
            Weight[] rArr = new Weight[results.size()];
            rArr = results.toArray(rArr);
            return rArr;
        }

        String content = this.data.get(ApiResource.WEIGHTS);
        results = this.apiResponseParser.getListWeights(content);
        Weight[] rArr = new Weight[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
    public MetricProfile[] getListMetrics() {
        List<MetricProfile> results = new ArrayList<MetricProfile>();
        if (!this.data.containsKey(ApiResource.METRIC)) {
            MetricProfile[] rArr = new MetricProfile[results.size()];
            rArr = results.toArray(rArr);
            return rArr;
        }

        String content = this.data.get(ApiResource.METRIC);

        results = this.apiResponseParser.getListMetrics(content);
        MetricProfile[] rArr = new MetricProfile[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * Executes all steps to retrieve the complete amount of the available
     * profile, topology, weights and downtime information from argo-web-api
     */
    public void getRemoteAll() {
        // Start with report and configuration
        this.getRemoteConfig();
        // parse remote report config to be able to get the other profiles

        parseReport();
        // Go on to the profiles
        this.getRemoteMetric();
        this.getRemoteOps();
        this.getRemoteAggregation();
        if (!this.threshID.equals("")) {
            this.getRemoteThresholds();
        }
        // Go to topology
        this.getRemoteTopoEndpoints();
        this.getRemoteTopoGroups();
        // get weights
        if (!this.weightsID.equals("")) {
            this.getRemoteWeights();
        }
        // get downtimes
        this.getRemoteDowntimes();
        // get recomptations
        this.getRemoteRecomputations();

    }

}
