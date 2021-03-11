package argo.functions.calculations;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.utils.TimelineBuilder;
import argo.pojos.TimelineTrends;
import argo.profileparsers.AggregationProfileParser.GroupOps;
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
public class CalcGroupFlipFlop implements GroupReduceFunction< TimelineTrends, TimelineTrends> {

    private HashMap<String,HashMap<String,String>> operationTruthTables;

    private String groupOperation;

    public CalcGroupFlipFlop(HashMap<String,HashMap<String,String>> operationTruthTables, String groupOperation) {
        this.operationTruthTables = operationTruthTables;
        this.groupOperation=groupOperation;
    }

    @Override
    public void reduce(Iterable<TimelineTrends> in, Collector< TimelineTrends> out) throws Exception {
        String group = null;
        String function = null;
        ArrayList<TimelineTrends> list = new ArrayList<>();
        //construct a timeline containing all the timestamps of each metric timeline

        TimelineBuilder timebuilder = new TimelineBuilder();

        ArrayList<TimelineTrends> timelist = new ArrayList<>();
        for (TimelineTrends time : in) {
            group = time.getGroup();
            function=time.getFunction();
            timelist.add(time);
        }
        
        HashMap<String, String> opTruthTable =  operationTruthTables.get(groupOperation);
    
        TreeMap<Date, String> resultMap = timebuilder.buildStatusTimeline(timelist, opTruthTable);
        int flipflops = timebuilder.calcFlipFlops(resultMap);

        TimelineTrends serviceFlipFlop = new TimelineTrends(group, resultMap, flipflops);
        out.collect(serviceFlipFlop);

    }

}
