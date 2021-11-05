package flipflops;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.pojos.Timeline;
//import argo.functions.calctimelines.TimelineMerger;
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
 * CalcEndpointTrends, count status changes for each service endpoint group
 */
public class CalcEndpointFlipFlopTrends implements FlatMapFunction<StatusTimeline, EndpointTrends> {

    public CalcEndpointFlipFlopTrends() {
    }

    static Logger LOG = LoggerFactory.getLogger(CalcEndpointFlipFlopTrends.class);

    /**
     *
     * @param in, a collection of MetricTrends as calculated on previous steps ,
     * from group, service, endpoint, metric groups
     * @param out, a collection of EndpointTrends containing the information of
     * the computation on group ,service, endpoint groups
     * @throws Exception
     */
    @Override
    public void flatMap(StatusTimeline in, Collector<EndpointTrends> out) throws Exception {
        String group = in.getGroup();
        String service = in.getService();
        String hostname = in.getHostname();
        //store the necessary info
        //collect all timelines in a list
        ArrayList<TimeStatus> timestatusList = in.getTimestamps();

        TreeMap<DateTime, Integer> timestampMap = new TreeMap();
        for (TimeStatus ts : timestatusList) {
            timestampMap.put(new DateTime(ts.getTimestamp()), ts.getStatus());
        }

        Timeline timeline = new Timeline();
        timeline.insertDateTimeStamps(timestampMap, true);
        HashMap<String, Timeline> timelineMap = new HashMap<>();
        timelineMap.put("timeline", timeline);
        Integer flipflop = timeline.calcStatusChanges();

        if (group != null && service != null && hostname != null) {

            EndpointTrends endpointTrends = new EndpointTrends(group, service, hostname, timeline, flipflop);
            out.collect(endpointTrends);
        }

    }
}
