package argo.functions.statustrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.avro.MetricData;
import java.text.ParseException;
import java.util.HashMap;
import java.util.TreeMap;
import argo.utils.Utils;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 *
 * CalcServiceEnpointMetricStatus, for each service endpoint metric group , keeps count
 * for each status (CRITICAL,WARNING,UNKNOW) appearance and returns the group
 * information (group, service,hostname, metric, status, statuscounter)
 */
public class TimelineStatusCounter extends RichGroupReduceFunction<MetricData, Tuple6<String, String, String, String, String, Integer>> {

    private transient HashMap<String, String> groupEndpoints;
    private String groupEndpointsPath;
    static Logger LOG = LoggerFactory.getLogger(TimelineStatusCounter.class);

    public TimelineStatusCounter(ParameterTool params) {
        this.groupEndpointsPath = params.getRequired("groupEndpointsPath");
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        groupEndpoints = Utils.readGroupEndpointJson(groupEndpointsPath); //contains the information of the (group, service) matches
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
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<MetricData> in, Collector<Tuple6<String, String, String, String, String, Integer>> out) throws ParseException {
        TreeMap<String, String> timeStatusMap = new TreeMap<>();
        String group = null;
        String hostname = null;
        String service = null;
        String status = null;
        String metric = null;
        int criticalSum = 0, warningSum = 0, unknownSum = 0;

        //for each MetricData in group check the status and increase counter accordingly
        for (MetricData md : in) {
            group = groupEndpoints.get(md.getHostname().toString() + "-" + md.getService().toString()); //retrieve the group for the service, as contained in file group_endpoints. if group is null exit 
            hostname = md.getHostname().toString();
            service = md.getService().toString();
            status = md.getStatus().toString();
            metric = md.getMetric().toString();
            if (group != null && service != null && hostname != null && metric != null) {

                if (status.equalsIgnoreCase("critical")) {
                    criticalSum++;
                } else if (status.equalsIgnoreCase("warning")) {

                    warningSum++;
                } else if (status.equalsIgnoreCase("unknown")) {
                    unknownSum++;
                }

            }
        }
        // for the group create result for each status and keep group info
        if (group != null && service != null && hostname != null && metric != null) {

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
