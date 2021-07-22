package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.avro.MetricData;
import argo.pojos.MetricTrends;
import argo.profiles.AggregationProfileManager;
import argo.profiles.EndpointGroupManager;
import argo.profiles.GroupGroupManager;
import argo.profiles.OperationsParser;
import argo.utils.Utils;
import java.util.ArrayList;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import timelines.Timeline;

/**
 *
 * @author cthermolia
 *
 * CalcMetricTrends, count status changes for each service endpoint metric
 */
public class CalcMetricFlipFlopTrends implements GroupReduceFunction<MetricData, MetricTrends> {

    private final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private EndpointGroupManager topologyEndpointParser;
    private GroupGroupManager topologyGroupParser;
    private AggregationProfileManager aggregationProfileParser;
    private OperationsParser operationsParser;
    private DateTime date;

    public CalcMetricFlipFlopTrends(OperationsParser operationsParser, EndpointGroupManager topologyEndpointParser, GroupGroupManager topologyGroupParser, AggregationProfileManager aggregationProfileParser, DateTime date) {
        this.topologyEndpointParser = topologyEndpointParser;
        this.aggregationProfileParser = aggregationProfileParser;
        this.operationsParser = operationsParser;
        this.topologyGroupParser = topologyGroupParser;
        this.date = date;
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
        TreeMap<DateTime, Integer> timeStatusMap = new TreeMap<>();
        ArrayList<String> groups = new ArrayList<>();
        String hostname = null;
        String service = null;
        String metric = null;
        String status = null;
        int criticalSum = 0, warningSum = 0, unknownSum = 0;

        for (MetricData md : in) {
            hostname = md.getHostname().toString();
            service = md.getService().toString();

            metric = md.getMetric().toString();
            status = md.getStatus().toString();
            //      group = groupEndpoints.get(md.getHostname().toString() + "-" + md.getService()); //retrieve the group for the service, as contained in file
            String avProfileName = this.aggregationProfileParser.getAvProfileItem().getName();

            // group = topologyEndpointParser.retrieveGroup(aggregationProfileParser.getProfileGroupType(avProfileName).toUpperCase(), md.getHostname().toString() + "-" + md.getService().toString());
            groups = topologyEndpointParser.getGroupFull(aggregationProfileParser.getProfileGroupType(avProfileName).toUpperCase(), md.getHostname().toString(), md.getService().toString());
            int st = operationsParser.getIntStatus(md.getStatus().toString());
            timeStatusMap.put(Utils.convertStringtoDate(format, md.getTimestamp().toString()), st);

            if (status.equalsIgnoreCase("critical")) {
                criticalSum++;
            } else if (status.equalsIgnoreCase("warning")) {
                warningSum++;
            } else if (status.equalsIgnoreCase("unknown")) {
                unknownSum++;
            }

        }

        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(timeStatusMap);

        timeline.replacePreviousDateStatus(date, new ArrayList<>(operationsParser.getStates().keySet()), true);//handle the first timestamp to contain the previous days timestamp status if necessary and the last timestamp to contain the status of the last timelines's entry
        Integer flipflop = timeline.calcStatusChanges();

        if (service != null && hostname != null && metric != null) {

            for (String group : groups) {

                if (topologyGroupParser.checkSubGroup(group)) {
                    MetricTrends metricTrends = new MetricTrends(group, service, hostname, metric, timeline, flipflop, criticalSum, warningSum, unknownSum);
                    out.collect(metricTrends);
                }
            }

        }
    }

}
