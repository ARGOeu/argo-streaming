package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import argo.functions.calctimelines.TimelineMerger;
import argo.pojos.GroupFunctionTrends;
import argo.pojos.ServiceTrends;
import argo.pojos.Timeline;
import argo.profiles.AggregationProfileParser;
import argo.profiles.OperationsParser;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 *
 * CalcGroupFunctionFlipFlop, count status changes for each group function
 * group
 */
public class CalcGroupFunctionFlipFlop implements GroupReduceFunction< ServiceTrends, GroupFunctionTrends> {

    private OperationsParser operationsParser;

    private HashMap<String,String> functionOperations;

    public CalcGroupFunctionFlipFlop(OperationsParser operationsParser, AggregationProfileParser aggregationProfileParser ) {
        this.operationsParser = operationsParser;
        this.functionOperations=aggregationProfileParser.getFunctionOperations();
    }

    @Override
    public void reduce(Iterable<ServiceTrends> in, Collector< GroupFunctionTrends> out) throws Exception {
        String group = null;
        String function = null;
       // ArrayList<Timeline> list = new ArrayList<>();
        //construct a timeline containing all the timestamps of each metric timeline

      
        ArrayList<Timeline> timelist = new ArrayList<>();
        for (ServiceTrends time : in) {
            group = time.getGroup();
            function=time.getFunction();
            timelist.add(time.getTimeline());
        }
        String operation=functionOperations.get(function);  //for each function an operation exists , so retrieve the corresponding truth table
        TimelineMerger timelineMerger = new TimelineMerger(operation,operationsParser);

      
        Timeline timeline= timelineMerger.mergeTimelines(timelist);
        int flipflops = timeline.calculateStatusChanges();

        GroupFunctionTrends groupFunctionTrends = new GroupFunctionTrends(group, function, timeline, flipflops);
        out.collect(groupFunctionTrends);

    }

}
