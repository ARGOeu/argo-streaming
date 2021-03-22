package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.functions.calctimelines.TimelineMerger;
import argo.pojos.EndpointTrends;
import argo.pojos.ServiceTrends;
import argo.pojos.Timeline;
import argo.profiles.AggregationProfileParser;
import argo.profiles.OperationsParser;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 *
 * CalcServiceEndpointFlipFlop, count status changes for each service endpoint
 * group
 */
public class CalcServiceFlipFlop implements GroupReduceFunction< EndpointTrends, ServiceTrends> {

    private HashMap<String, HashMap<String, String>> operationTruthTables;

    private HashMap<String, String> serviceOperationMap;
    private OperationsParser operationsParser;

    public CalcServiceFlipFlop(OperationsParser operationsParser, AggregationProfileParser aggregationProfileParser) {
//        this.operationTruthTables = operationParser.getOpTruthTable();
        this.operationsParser = operationsParser;
        this.serviceOperationMap = aggregationProfileParser.getServiceOperations();
    }

    @Override
    public void reduce(Iterable<EndpointTrends> in, Collector< ServiceTrends> out) throws Exception {
        String group = null;
        String service = null;
        // String hostname = null;
        ArrayList<ServiceTrends> list = new ArrayList<>();
        //construct a timeline containing all the timestamps of each metric timeline

       
        ArrayList<Timeline> timelineList = new ArrayList<>();
        for (EndpointTrends endpointTrend : in) {
            group = endpointTrend.getGroup();
            service = endpointTrend.getService();
            timelineList.add(endpointTrend.getTimeline());
        }
        String operation = serviceOperationMap.get(service);
        TimelineMerger timelineMerger = new TimelineMerger(operation, operationsParser);

///        HashMap<String, String> opTruthTable = operationTruthTables.get(operation);
        Timeline timeline = timelineMerger.mergeTimelines(timelineList);
        int flipflops = timeline.calculateStatusChanges();

        ServiceTrends serviceTrends = new ServiceTrends(group, service, timeline, flipflops);
        out.collect(serviceTrends);

    }

}
