package argo.commons.profiles;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.util.Objects;
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
    private DowntimeParser downtimeParser;

    public ProfilesLoader() {
        
    }
//apiUri, required  -key required ---proxy --reportId required --date required
    
    public ProfilesLoader(String apiUri,String key, String proxy,String reportId,String date)throws IOException, ParseException, java.text.ParseException {

        reportParser = new ReportParser(apiUri, key, proxy, reportId);
        String[] reportInfo = reportParser.getTenantReport().getInfo();
        topolGroupParser = new TopologyGroupParser(apiUri,key, proxy, date,reportInfo[0]);
        topologyEndpointParser = new TopologyEndpointParser(apiUri, key, proxy,  date,reportInfo[0]);
       
        String aggregationId = reportParser.getAggregationReportId();
        String metricId = reportParser.getMetricReportId();
        String operationsId = reportParser.getOperationReportId();

        aggregationProfileParser = new AggregationProfileParser(apiUri, key, proxy, aggregationId, date);
        metricProfileParser = new MetricProfileParser(apiUri, key,proxy, metricId, date);
        operationParser = new OperationsParser(apiUri, key, proxy, operationsId, date);
         downtimeParser = new DowntimeParser(apiUri, key, proxy, date);

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

    public TopologyGroupParser getTopolGroupParser() {
        return topolGroupParser;
    }

    public void setTopolGroupParser(TopologyGroupParser topolGroupParser) {
        this.topolGroupParser = topolGroupParser;
    }

    public DowntimeParser getDowntimeParser() {
        return downtimeParser;
    }

    public void setDowntimeParser(DowntimeParser downtimeParser) {
        this.downtimeParser = downtimeParser;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.reportParser);
        hash = 97 * hash + Objects.hashCode(this.topologyEndpointParser);
        hash = 97 * hash + Objects.hashCode(this.metricProfileParser);
        hash = 97 * hash + Objects.hashCode(this.operationParser);
        hash = 97 * hash + Objects.hashCode(this.aggregationProfileParser);
        hash = 97 * hash + Objects.hashCode(this.topolGroupParser);
        hash = 97 * hash + Objects.hashCode(this.downtimeParser);
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
        final ProfilesLoader other = (ProfilesLoader) obj;
        if (!Objects.equals(this.reportParser, other.reportParser)) {
            return false;
        }
        if (!Objects.equals(this.topologyEndpointParser, other.topologyEndpointParser)) {
            return false;
        }
        if (!Objects.equals(this.metricProfileParser, other.metricProfileParser)) {
            return false;
        }
        if (!Objects.equals(this.operationParser, other.operationParser)) {
            return false;
        }
        if (!Objects.equals(this.aggregationProfileParser, other.aggregationProfileParser)) {
            return false;
        }
        if (!Objects.equals(this.topolGroupParser, other.topolGroupParser)) {
            return false;
        }
        if (!Objects.equals(this.downtimeParser, other.downtimeParser)) {
            return false;
        }
        return true;
    }
    

}
