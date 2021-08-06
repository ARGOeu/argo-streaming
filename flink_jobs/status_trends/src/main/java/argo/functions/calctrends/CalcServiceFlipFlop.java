package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.EndpointTrends;
import argo.pojos.ServiceTrends;
import argo.profiles.AggregationProfileManager;
import argo.profiles.OperationsParser;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import timelines.Timeline;
import timelines.TimelineAggregator;

/**
 *
 * @author cthermolia
 *
 * CalcServiceEndpointFlipFlop, count status changes for each service endpoint
 * group
 */
public class CalcServiceFlipFlop implements GroupReduceFunction< EndpointTrends, ServiceTrends> {

    private HashMap<String, HashMap<String, String>> operationTruthTables;

    private HashMap<String, String> serviceFunctionsMap;
    private OperationsParser operationsParser;

    public CalcServiceFlipFlop(OperationsParser operationsParser, AggregationProfileManager aggregationProfileParser) {
//        this.operationTruthTables = operationParser.getOpTruthTable();
        this.operationsParser = operationsParser;
        this.serviceFunctionsMap = aggregationProfileParser.retrieveServiceOperations();
        
    }

    @Override
    public void reduce(Iterable<EndpointTrends> in, Collector< ServiceTrends> out) throws Exception {
        String group = null;
        String service = null;
        // String hostname = null;
        ArrayList<ServiceTrends> list = new ArrayList<>();
        //construct a timeline containing all the timestamps of each metric timeline

        HashMap<String, Timeline> timelineList = new HashMap<>();

        for (EndpointTrends endpointTrend : in) {
            group = endpointTrend.getGroup();
            service = endpointTrend.getService();
            timelineList.put(endpointTrend.getEndpoint(), endpointTrend.getTimeline());
        }
        String operation = serviceFunctionsMap.get(service);
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelineList);
        timelineAggregator.aggregate(operationsParser.getTruthTable(), operationsParser.getIntOperation(operation));


        Timeline timeline = timelineAggregator.getOutput();
        int flipflops = timeline.calcStatusChanges();
        if (group != null && service != null) {
            ServiceTrends serviceTrends = new ServiceTrends(group, service, timeline, flipflops);
            out.collect(serviceTrends);
        }
    }
}
