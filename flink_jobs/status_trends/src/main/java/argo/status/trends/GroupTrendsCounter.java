package argo.status.trends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.GroupTrends;
import argo.profiles.OperationsParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import timelines.Timeline;

/**
 * GroupTrendsCounter calculates on the dataset's timeline the num of
 * appearances of the status CRITICAL, WARNING,UNKNOWN and produces a dataset of
 * tuples that contain these calculations
 */
public class GroupTrendsCounter implements FlatMapFunction<GroupTrends, Tuple7< String, String, String, String, String, Integer, Integer>> {

    private OperationsParser operationsParser;

    public GroupTrendsCounter(OperationsParser operationsParser) {
        this.operationsParser = operationsParser;
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
    public void flatMap(GroupTrends t, Collector< Tuple7<String, String, String, String, String, Integer, Integer>> out) throws Exception {

        int criticalstatus = operationsParser.getIntStatus("CRITICAL");
        int warningstatus = operationsParser.getIntStatus("WARNING");
        int unknownstatus = operationsParser.getIntStatus("UNKNOWN");

        Timeline timeline = t.getTimeline();
        int[] criticalstatusInfo = timeline.countStatusAppearances(criticalstatus);
        int[] warningstatusInfo = timeline.countStatusAppearances(warningstatus);
        int[] unknownstatusInfo = timeline.countStatusAppearances(unknownstatus);

        Tuple7< String, String, String, String, String, Integer, Integer> tupleCritical = new Tuple7<  String, String, String, String, String, Integer, Integer>(
                t.getGroup(), null, null, null, "CRITICAL", criticalstatusInfo[0], criticalstatusInfo[1]);
        out.collect(tupleCritical);

        Tuple7<  String, String, String, String, String, Integer, Integer> tupleWarning = new Tuple7< String, String, String, String, String, Integer, Integer>(
                t.getGroup(), null, null, null, "WARNING", warningstatusInfo[0], warningstatusInfo[1]);

        out.collect(tupleWarning);

        Tuple7<  String, String, String, String, String, Integer, Integer> tupleUnknown = new Tuple7<  String, String, String, String, String, Integer, Integer>(
                t.getGroup(), null, null, null, "UNKNOWN", unknownstatusInfo[0], unknownstatusInfo[1]);
        out.collect(tupleUnknown);

    }
}
