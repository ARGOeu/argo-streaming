package trends.calculations;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.batch.StatusTimeline;
import argo.batch.TimeStatus;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timelines.Timeline;

/**
 * CalcServiceEndpointFlipFlop, count status changes for each service endpoint
 * group
 */
public class CalcServiceFlipFlopTrends implements FlatMapFunction<StatusTimeline, ServiceTrends> {

    public CalcServiceFlipFlopTrends() {

    }
    static Logger LOG = LoggerFactory.getLogger(CalcServiceFlipFlopTrends.class);

    @Override
    public void flatMap(StatusTimeline in, Collector<ServiceTrends> out) throws Exception {

        String group = in.getGroup();
        String service = in.getService();
        //construct a timeline containing all the timestamps of each metric timeline
        ArrayList<TimeStatus> timestatusList = in.getTimestamps();

        TreeMap<DateTime, Integer> timestampMap = new TreeMap();
        for (TimeStatus ts : timestatusList) {
            timestampMap.put(new DateTime(ts.getTimestamp()), ts.getStatus());
        }
        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(timestampMap, true);
        HashMap<String, Timeline> timelineMap = new HashMap<>();
        timelineMap.put("timeline", timeline);
        int flipflop = timeline.calcStatusChanges();

        if (group != null && service != null) {
            ServiceTrends serviceTrends = new ServiceTrends(group, service, timeline, flipflop);
            out.collect(serviceTrends);
        }
    }

}
