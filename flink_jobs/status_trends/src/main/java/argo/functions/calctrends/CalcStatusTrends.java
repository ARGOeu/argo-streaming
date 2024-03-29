package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.avro.MetricData;
import argo.profiles.AggregationProfileManager;
import argo.profiles.EndpointGroupManager;
import java.text.ParseException;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

/**
 * CalcServiceEnpointMetricStatus, for each service endpoint metric group ,
 * keeps count for each status (CRITICAL,WARNING,UNKNOW) appearance and returns
 * the group information (group, service,hostname, metric, status,
 * statuscounter)
 */
public class CalcStatusTrends implements GroupReduceFunction<MetricData, Tuple6<String, String, String, String, String, Integer>> {

    private EndpointGroupManager topologyEndpointParser;
    private AggregationProfileManager aggregationProfileParser;


    public CalcStatusTrends(EndpointGroupManager topologyEndpointParser, AggregationProfileManager aggregationProfileParser) {
        this.topologyEndpointParser = topologyEndpointParser;
        this.aggregationProfileParser = aggregationProfileParser;

    }

    /**
     * for each service endpoint metric Iterable check the MetricData status ,
     * keep counter for each status(CRITICAL, WARNING,UNKNOWN) and provide
     * results for each status
     *
     * @param in, the MetricData dataset
     * @param out, the collection of 3 Tuple6 for each Iterable (one for each
     * status) that keeps info for group ,service, hostname, metric, status, and
     * status counter
     */
    @Override
    public void reduce(Iterable<MetricData> in, Collector<Tuple6<String, String, String, String, String, Integer>> out) throws ParseException {
        ArrayList<String> groups = new ArrayList<>();
        String hostname = null;
        String service = null;
        String status = null;
        String metric = null;
        int criticalSum = 0, warningSum = 0, unknownSum = 0;

        //for each MetricData in group check the status and increase counter accordingly
        for (MetricData md : in) {
        String avProfileName = this.aggregationProfileParser.getAvProfileItem().getName();

            groups = topologyEndpointParser.getGroupFull(aggregationProfileParser.getProfileGroupType(avProfileName).toUpperCase(), md.getHostname().toString(), md.getService().toString());

            hostname = md.getHostname().toString();
            service = md.getService().toString();
            status = md.getStatus().toString();
            metric = md.getMetric().toString();
            if (service != null && hostname != null && metric != null) {
                for (String group : groups) {
                    if (status.equalsIgnoreCase("critical")) {
                        criticalSum++;
                    } else if (status.equalsIgnoreCase("warning")) {

                        warningSum++;
                    } else if (status.equalsIgnoreCase("unknown")) {
                        unknownSum++;
                    }

                }
            }
        }
        // for the group create result for each status and keep group info
        if ( service != null && hostname != null && metric != null) {
            for(String group: groups){
            Tuple6<String, String, String, String, String, Integer> tupleCritical = new Tuple6<String, String, String, String, String, Integer>(
                    group, service, hostname, metric, "CRITICAL", criticalSum);
            out.collect(tupleCritical);

            Tuple6<String, String, String, String, String, Integer> tupleWarning = new Tuple6<String, String, String, String, String, Integer>(
                    group, service, hostname, metric, "WARNING", warningSum);
            out.collect(tupleWarning);

            Tuple6<String, String, String, String, String, Integer> tupleUnknown = new Tuple6<String, String, String, String, String, Integer>(
                    group, service, hostname, metric, "UNKNOWN", unknownSum);
            out.collect(tupleUnknown);
            }
        }
    }

}
