package argo.functions.calctrends;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.functions.calctimelines.TimelineMerger;
import argo.pojos.GroupFunctionTrends;
import argo.pojos.GroupTrends;
import argo.profiles.AggregationProfileManager;
import argo.profiles.OperationsParser;
import java.util.HashMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import timelines.Timeline;
import timelines.TimelineAggregator;

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

    public CalcGroupFlipFlop(OperationsParser operationsParser, AggregationProfileManager aggregationProfileParser) {
        this.operationsParser = operationsParser;
        this.groupOperation = aggregationProfileParser.retrieveProfileOperation();
    }

    @Override
    public void reduce(Iterable<GroupFunctionTrends> in, Collector< GroupTrends> out) throws Exception {
        String group = null;

        HashMap<String, Timeline> timelist = new HashMap<>();
        for (GroupFunctionTrends time : in) {
            group = time.getGroup();
            timelist.put(time.getFunction(), time.getTimeline());
        }
        TimelineAggregator timelineAggregator = new TimelineAggregator(timelist);

        timelineAggregator.aggregate(operationsParser.getTruthTable(), operationsParser.getIntOperation(groupOperation));
        Timeline timeline = timelineAggregator.getOutput();
        int flipflops = timeline.calcStatusChanges();

        GroupTrends groupTrends = new GroupTrends(group, timeline, flipflops);
        out.collect(groupTrends);
    }

}
