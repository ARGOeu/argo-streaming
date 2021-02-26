/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.timeline;

import argo.avro.MetricData;
import argo.pojos.MetricTimelinePojo;
import argo.utils.Utils;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * @author cthermolia
 *
 * CalcMetricTimelineStatus calculates the timeline for each group, service,
 * endpoint , metric
 */
public class CalcMetricTimelineStatus implements GroupReduceFunction<MetricData, MetricTimelinePojo> {

    private HashMap<String, String> groupEndpoints;

    public CalcMetricTimelineStatus(HashMap<String, String> groupEndpoints) {
        this.groupEndpoints = groupEndpoints;
    }

    @Override
    public void reduce(Iterable<MetricData> in, Collector<MetricTimelinePojo> out) throws Exception {

        String group = null;
        String service = null;
        String hostname = null;
        String metric = null;
        ArrayList<Date> timeline = new ArrayList<>();
        TreeMap<Date, String> mapTimeline = new TreeMap<Date, String>();
        for (MetricData md : in) { //for each metric in group
            hostname = md.getHostname().toString();
            service = md.getService().toString();
            group = groupEndpoints.get(md.getHostname().toString() + "-" + md.getService().toString()); //gets the group name contained in the group endpoint input
            metric = md.getMetric().toString();
            // add time and status to timeline
            Date timestamp = Utils.convertStringtoDate(md.getTimestamp().toString());
            mapTimeline.put(timestamp, md.getStatus().toString());
        }
        MetricTimelinePojo mt = new MetricTimelinePojo(group, service, metric, metric, mapTimeline);
        out.collect(mt);
    }

}
