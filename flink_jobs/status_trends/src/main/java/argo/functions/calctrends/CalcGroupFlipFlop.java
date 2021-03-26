package argo.functions.calctrends;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.functions.calctimelines.TimelineMerger;
import argo.pojos.GroupFunctionTrends;
import argo.pojos.GroupTrends;
import argo.pojos.Timeline;
import argo.profiles.AggregationProfileParser;
import argo.profiles.OperationsParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 *
 * CalcServiceEndpointFlipFlop, count status changes for each service endpoint
 * group
 */
public class CalcGroupFlipFlop implements GroupReduceFunction< GroupFunctionTrends, GroupTrends> {

    private OperationsParser operationsParser;
    private String groupOperation;

    public CalcGroupFlipFlop(OperationsParser operationsParser, AggregationProfileParser aggregationProfileParser) {
        this.operationsParser = operationsParser;
        this.groupOperation = aggregationProfileParser.getProfileOp();
    }

    @Override
    public void reduce(Iterable<GroupFunctionTrends> in, Collector< GroupTrends> out) throws Exception {
        String group = null;

        ArrayList<Timeline> timelist = new ArrayList<>();
        for (GroupFunctionTrends time : in) {
            group = time.getGroup();
            timelist.add(time.getTimeline());
        }
        TimelineMerger timelineMerger = new TimelineMerger(groupOperation, operationsParser);

        Timeline timeline = timelineMerger.mergeTimelines(timelist);
        int flipflops = timeline.calculateStatusChanges();

        GroupTrends groupTrends = new GroupTrends(group, timeline, flipflops);
        out.collect(groupTrends);

    }

}
