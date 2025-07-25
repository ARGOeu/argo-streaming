package trends.status;



/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import trends.calculations.GroupTrends;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import profilesmanager.OperationsManager;
import timelines.Timeline;
import timelines.TimelineIntegrator;

/**
 * GroupTrendsCounter calculates on the dataset's timeline the num of
 * appearances of the status CRITICAL, WARNING,UNKNOWN and produces a dataset of
 * tuples that contain these calculations
 */
public class  GroupTrendsCounter extends RichFlatMapFunction<GroupTrends, Tuple8< String, String, String, String, String, Integer, Integer,String>> {

   private List<String> ops;

    private OperationsManager opsMgr;

    public GroupTrendsCounter() {
    }

    @Override
    public void open(Configuration parameters) throws IOException {

          this.ops = getRuntimeContext().getBroadcastVariable("ops");
          // Initialize operations manager
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);
    }

    /**
     * if the service exist in one or more function groups , timeline trends are
     * produced for each function that the service belongs and the function info
     * is added to the timeline trend
     *
     * @param t
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(GroupTrends t, Collector< Tuple8< String, String, String, String, String, Integer, Integer,String>> out) throws Exception {

        int criticalstatus = this.opsMgr.getIntStatus("CRITICAL");
        int warningstatus = this.opsMgr.getIntStatus("WARNING");
        int unknownstatus = this.opsMgr.getIntStatus("UNKNOWN");

             Timeline timeline = t.getTimeline();
        TimelineIntegrator timelineIntegrator = new TimelineIntegrator();
        int[] criticalstatusInfo = timelineIntegrator.countStatusAppearances(timeline.getSamples(), criticalstatus);
        int[] warningstatusInfo = timelineIntegrator.countStatusAppearances(timeline.getSamples(), warningstatus);
        int[] unknownstatusInfo = timelineIntegrator.countStatusAppearances(timeline.getSamples(), unknownstatus);


        Tuple8< String, String, String, String, String, Integer, Integer,String> tupleCritical = new Tuple8< String, String, String, String, String, Integer, Integer,String>(
                t.getGroup(), null, null, null, "CRITICAL", criticalstatusInfo[0], criticalstatusInfo[1],"");
        out.collect(tupleCritical);

       Tuple8< String, String, String, String, String, Integer, Integer,String> tupleWarning = new Tuple8< String, String, String, String, String, Integer, Integer,String>(
                t.getGroup(), null, null, null, "WARNING", warningstatusInfo[0], warningstatusInfo[1],"");


        out.collect(tupleWarning);

        Tuple8< String, String, String, String, String, Integer, Integer,String> tupleUnknown = new Tuple8< String, String, String, String, String, Integer, Integer,String>(
                t.getGroup(), null, null, null, "UNKNOWN", unknownstatusInfo[0], unknownstatusInfo[1],"");
        out.collect(tupleUnknown);

    }
}
