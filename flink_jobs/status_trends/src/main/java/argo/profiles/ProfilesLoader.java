/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;

import argo.utils.RequestManager;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import java.io.IOException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.json.simple.parser.ParseException;

/**
 * ProfilesLoader, loads all the parser that will be used to collect the
 * information from the web api
 */
public class ProfilesLoader {


    private ReportManager reportParser;

    private EndpointGroupManager topologyEndpointParser;
    private MetricProfileManager metricProfileParser;

    private OperationsParser operationParser;
    private AggregationProfileManager aggregationProfileParser;

    private GroupGroupManager topolGroupParser;

    private String aggregationId;
    private String metricId;
    private String operationsId;

    public ProfilesLoader() {
    }

    public ProfilesLoader(ParameterTool params) throws IOException, ParseException {
       JsonElement reportProfileJson = RequestManager.reportProfileRequest(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), params.getRequired("reportId"));
        reportParser = new ReportManager();
        reportParser.readJson(reportProfileJson);

        String reportName = reportParser.getReport();

        aggregationId = reportParser.getAggregationReportId();
        metricId = reportParser.getMetricReportId();
        operationsId = reportParser.getOperationReportId();

        JsonElement opProfileJson = RequestManager.operationsProfileRequest(params.getRequired("apiUri"), operationsId, params.getRequired("key"), params.get("proxy"), params.get("date"));

        operationParser = new OperationsParser();
        operationParser.readJson(opProfileJson);

        JsonElement metricProfileJson = RequestManager.metricProfileRequest(params.getRequired("apiUri"), metricId, params.getRequired("key"), params.get("proxy"), params.get("date"));

        metricProfileParser = new MetricProfileManager();
        metricProfileParser.loadMetricProfile(metricProfileJson);

        JsonElement aggregationProfileJson = RequestManager.aggregationProfileRequest(params.getRequired("apiUri"), aggregationId, params.getRequired("key"), params.get("proxy"), params.get("date"));

        aggregationProfileParser = new AggregationProfileManager();
        aggregationProfileParser.readJson(aggregationProfileJson);

        JsonArray endpointGroupProfileJson = RequestManager.endpointGroupProfileRequest(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), reportName, params.get("date"));

        topologyEndpointParser = new EndpointGroupManager();
        topologyEndpointParser.loadGroupEndpointProfile(endpointGroupProfileJson);

        JsonArray groupGroupProfileJson = RequestManager.groupGroupProfileRequest(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), reportName, params.get("date"));

        topolGroupParser = new GroupGroupManager();
        topolGroupParser.loadGroupGroupProfile(groupGroupProfileJson);
    }


    public ReportManager getReportParser() {
        return reportParser;
    }

    public void setReportParser(ReportManager reportParser) {
        this.reportParser = reportParser;
    }

    public EndpointGroupManager getTopologyEndpointParser() {
        return topologyEndpointParser;
    }

    public void setTopologyEndpointParser(EndpointGroupManager topologyEndpointParser) {
        this.topologyEndpointParser = topologyEndpointParser;
    }

    public MetricProfileManager getMetricProfileParser() {
        return metricProfileParser;
    }

    public void setMetricProfileParser(MetricProfileManager metricProfileParser) {
        this.metricProfileParser = metricProfileParser;
    }

    public OperationsParser getOperationParser() {
        return operationParser;
    }

    public void setOperationParser(OperationsParser operationParser) {
        this.operationParser = operationParser;
    }

    public AggregationProfileManager getAggregationProfileParser() {
        return aggregationProfileParser;
    }
    public void setAggregationProfileParser(AggregationProfileManager aggregationProfileParser) {
        this.aggregationProfileParser = aggregationProfileParser;
    }
    public GroupGroupManager getTopolGroupParser() {
        return topolGroupParser;
    }

    public void setTopolGroupParser(GroupGroupManager topolGroupParser) {
        this.topolGroupParser = topolGroupParser;
    }

    public String getAggregationId() {
        return aggregationId;
    }

    public void setAggregationId(String aggregationId) {
        this.aggregationId = aggregationId;
    }

    public String getMetricId() {
        return metricId;
    }

    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    public String getOperationsId() {
        return operationsId;
    }

    public void setOperationsId(String operationsId) {
        this.operationsId = operationsId;
    }

}
