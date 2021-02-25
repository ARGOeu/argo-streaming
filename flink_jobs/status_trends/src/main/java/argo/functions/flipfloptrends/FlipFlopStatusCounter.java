package argo.functions.flipfloptrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.avro.MetricData;
import argo.utils.Utils;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 *
 * CalcServiceEnpointMetricFlipFlop, count status changes for each service endpoint metric
 */
public class FlipFlopStatusCounter extends RichGroupReduceFunction<MetricData, Tuple5<String, String, String, String, Integer>> {

    static Logger LOG = LoggerFactory.getLogger(FlipFlopStatusCounter.class);

    private transient HashMap<String, String> groupEndpoints;
    private String groupEndpointsPath;

    public FlipFlopStatusCounter(ParameterTool params) {
        this.groupEndpointsPath = params.getRequired("groupEndpointsPath");
    }

    @Override
    public void open(Configuration config) throws Exception {
        groupEndpoints = Utils.readGroupEndpointJson(groupEndpointsPath); //contains the information of the (group, service) matches
    }

    /**
     *
     * @param in, the MetricData dataset
     * @param out, the coll ection of Tuple5, keeping counter for each group
     * service hostname metric
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<MetricData> in, Collector<Tuple5<String, String, String, String, Integer>> out) throws Exception {
        TreeMap<String, String> timeStatusMap = new TreeMap<>();
        String group = null;
        String hostname = null;
        String service = null;
        String metric = null;

        for (MetricData md : in) {
            hostname = md.getHostname().toString();
            service = md.getService().toString();
            metric = md.getMetric().toString();
            group = groupEndpoints.get(md.getHostname().toString() + "-" + md.getService().toString()); //retrieve the group for the service, as contained in file
            timeStatusMap.put(md.getTimestamp().toString(), md.getStatus().toString());
        }
        timeStatusMap = handleFirstLastTimestamps(timeStatusMap);
        int flipflop = calcFlipFlops(timeStatusMap);
        if (group != null && service != null && hostname != null && metric != null) {
            Tuple5<String, String, String, String, Integer> tuple = new Tuple5<String, String, String, String, Integer>(
                    group, service, hostname, metric, flipflop
            );
            out.collect(tuple);
        }
    }

// add to map an entry of (T00:00:00, status) and an entry of (T23:59:59, status) 
    private TreeMap<String, String> handleFirstLastTimestamps(TreeMap<String, String> map) throws ParseException {
        Map.Entry<String, String> firstEntry = map.firstEntry();
        Map.Entry<String, String> lastEntry = map.lastEntry();
        String status = firstEntry.getValue();
        String timestamp = Utils.createDate(lastEntry.getKey(), 0, 0, 0);
        if (Utils.isPreviousDate(timestamp, firstEntry.getKey())) {  //if exists a previous day timestamp get the status of the timestamp, else set the status=MISSING
            status = firstEntry.getValue();
        } else {
            status = "MISSING";
        }

        map.put(timestamp, status);

        status = lastEntry.getValue();
        timestamp = Utils.createDate(lastEntry.getKey(), 23, 59, 59);
        map.put(timestamp, status);
        map.remove(firstEntry.getKey());
        return map;
    }
// calculate status changes
    private int calcFlipFlops(TreeMap<String, String> map) {

        String previousStatus = null;
        int flipflop = 0;
        for (Entry<String, String> entry : map.entrySet()) {
            String status = entry.getValue();
            if (previousStatus != null && !status.equalsIgnoreCase(previousStatus)) {
                flipflop++;
            }
            previousStatus = status;
        }
        return flipflop;
    }

}
