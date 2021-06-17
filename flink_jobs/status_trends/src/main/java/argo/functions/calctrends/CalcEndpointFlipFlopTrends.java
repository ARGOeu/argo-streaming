package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.pojos.Timeline;
//import argo.functions.calctimelines.TimelineMerger;
import argo.pojos.EndpointTrends;
import argo.pojos.MetricTrends;
import argo.profiles.OperationsParser;

import java.util.HashMap;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import timelines.Timeline;
import timelines.TimelineAggregator;

/**
 * CalcEndpointTrends, count status changes for each service endpoint group
 */
public class CalcEndpointFlipFlopTrends extends RichGroupReduceFunction<MetricTrends, EndpointTrends> {

    private OperationsParser operationsParser;
    private String operation;
    private DateTime date;

    public CalcEndpointFlipFlopTrends(String operation, OperationsParser operationsParser) {
        this.operation = operation;
        this.operationsParser = operationsParser;

    }

    /**
     *
     * @param in, a collection of MetricTrends as calculated on previous steps ,
     * from group, service, endpoint, metric groups
     * @param out, a collection of EndpointTrends containing the information of
     * the computation on group ,service, endpoint groups
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<MetricTrends> in, Collector< EndpointTrends> out) throws Exception {
        String group = null;
        String service = null;
        String hostname = null;
        //store the necessary info
        //collect all timelines in a list

        HashMap<String, Timeline> timelinelist = new HashMap<>();

        for (MetricTrends time : in) {
            group = time.getGroup();
            service = time.getService();
            hostname = time.getEndpoint();
            Timeline timeline = time.getTimeline();
            timelinelist.put(time.getMetric(), timeline);

        }
        // merge the timelines into one timeline ,  

        // as multiple status (each status exist in each timeline) correspond to each timestamp, there is a need to conclude into one status/timestamp
        //for each timestamp the status that prevails is concluded by the truth table that is defined for the operation
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelinelist);
        timelineAggregator.aggregate(operationsParser.getTruthTable(), operationsParser.getIntOperation(operation));

        Timeline mergedTimeline = timelineAggregator.getOutput(); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline
        Integer flipflops = mergedTimeline.calcStatusChanges();//calculate flip flops on the concluded merged timeline
        if (group != null && service != null && hostname != null) {
            EndpointTrends endpointTrends = new EndpointTrends(group, service, hostname, mergedTimeline, flipflops);
            out.collect(endpointTrends);
        }
    }
}
