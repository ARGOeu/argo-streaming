package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.avro.MetricData;
import argo.pojos.Timeline;
import argo.pojos.MetricTrends;
import argo.profiles.AggregationProfileParser;
import argo.profiles.TopologyEndpointParser;
import argo.utils.Utils;
import java.util.Date;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 *
 * CalcMetricTrends, count status changes for each service endpoint metric
 */
public class CalcMetricFlipFlopTrends implements GroupReduceFunction<MetricData, MetricTrends> {

    //private HashMap<String, String> groupEndpoints;
    private final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private TopologyEndpointParser topologyEndpointParser;
    private AggregationProfileParser aggregationProfileParser;
//
//    public CalcMetricFlipFlopTrends(HashMap<String, String> groupEndpoints) {
//        this.groupEndpoints = groupEndpoints;
//    }

    public CalcMetricFlipFlopTrends(TopologyEndpointParser topologyEndpointParser, AggregationProfileParser aggregationProfileParser) {
        this.topologyEndpointParser = topologyEndpointParser;
        this.aggregationProfileParser = aggregationProfileParser;
    }

    /**
     *
     * @param in, the MetricData dataset
     * @param out, the collection of MetricTrends, containing the information of
     * the computations on group, service, endpoint, metric groups
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<MetricData> in, Collector<MetricTrends> out) throws Exception {
        TreeMap<Date, String> timeStatusMap = new TreeMap<>();
        String group = null;
        String hostname = null;
        String service = null;
        String metric = null;

        for (MetricData md : in) {
            hostname = md.getHostname().toString();
            service = md.getService().toString();
            metric = md.getMetric().toString();
            //      group = groupEndpoints.get(md.getHostname().toString() + "-" + md.getService()); //retrieve the group for the service, as contained in file
            group = topologyEndpointParser.retrieveGroup(aggregationProfileParser.getEndpointGroup().toUpperCase(), md.getHostname().toString() + "-" + md.getService().toString());
            timeStatusMap.put(Utils.convertStringtoDate(format, md.getTimestamp().toString()), md.getStatus().toString());
        }
        Timeline timeline = new Timeline(timeStatusMap);
        timeline.manageFirstLastTimestamps(); //handle the first timestamp to contain the previous days timestamp status if necessary and the last timestamp to contain the status of the last timelines's entry
        Integer flipflop = timeline.calculateStatusChanges();

        if (group != null && service != null && hostname != null && metric != null) {
            MetricTrends metricTrends = new MetricTrends(group, service, hostname, metric, timeline, flipflop);
            out.collect(metricTrends);
        }
    }
}
