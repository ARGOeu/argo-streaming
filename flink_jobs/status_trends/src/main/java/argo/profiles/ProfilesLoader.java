/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;
import java.io.IOException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 * 
 * ProfilesLoader, loads all the parser that will be used to collect the information from the web api
 */
public class ProfilesLoader {

    private ReportParser reportParser;
    private TopologyEndpointParser topologyEndpointParser;
    private MetricProfileParser metricProfileParser;
    private OperationsParser operationParser;
    private AggregationProfileParser aggregationProfileParser;
    private TopologyGroupParser topolGroupParser;

    public ProfilesLoader() {
    }

    
    public ProfilesLoader(ParameterTool params) throws IOException, ParseException {

        reportParser = new ReportParser(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), params.getRequired("reportId"));
        String[] reportInfo = reportParser.getTenantReport().getInfo();
        topolGroupParser = new TopologyGroupParser(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), params.getRequired("date"),reportInfo[0]);
        topologyEndpointParser = new TopologyEndpointParser(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"),  params.getRequired("date"),reportInfo[0]);
       
        String aggregationId = reportParser.getAggregationReportId();
        String metricId = reportParser.getMetricReportId();
        String operationsId = reportParser.getOperationReportId();

        aggregationProfileParser = new AggregationProfileParser(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), aggregationId, params.get("date"));
        metricProfileParser = new MetricProfileParser(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), metricId, params.get("date"));
        operationParser = new OperationsParser(params.getRequired("apiUri"), params.getRequired("key"), params.get("proxy"), operationsId, params.get("date"));

    }

    public ReportParser getReportParser() {
        return reportParser;
    }

    public void setReportParser(ReportParser reportParser) {
        this.reportParser = reportParser;
    }

    public TopologyEndpointParser getTopologyEndpointParser() {
        return topologyEndpointParser;
    }

    public void setTopologyEndpointParser(TopologyEndpointParser topologyEndpointParser) {
        this.topologyEndpointParser = topologyEndpointParser;
    }

    public MetricProfileParser getMetricProfileParser() {
        return metricProfileParser;
    }

    public void setMetricProfileParser(MetricProfileParser metricProfileParser) {
        this.metricProfileParser = metricProfileParser;
    }

    public OperationsParser getOperationParser() {
        return operationParser;
    }

    public void setOperationParser(OperationsParser operationParser) {
        this.operationParser = operationParser;
    }

    public AggregationProfileParser getAggregationProfileParser() {
        return aggregationProfileParser;
    }

    public void setAggregationProfileParser(AggregationProfileParser aggregationProfileParser) {
        this.aggregationProfileParser = aggregationProfileParser;
    }

}
