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
 * CalcGroupFunctionFlipFlop, count status changes for each group function
 * group
 */
public class CalcGroupFunctionFlipFlop implements GroupReduceFunction< TimelineTrends, TimelineTrends> {

    private HashMap<String,HashMap<String,String>> operationTruthTables;

    private HashMap<String,String> functionOperations;

    public CalcGroupFunctionFlipFlop(HashMap<String,HashMap<String,String>> operationTruthTables, HashMap<String,String> functionOperations) {
        this.operationTruthTables = operationTruthTables;
        this.functionOperations=functionOperations;
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
        String operation=functionOperations.get(function);  //for each function an operation exists , so retrieve the corresponding truth table
        
        HashMap<String, String> opTruthTable =  operationTruthTables.get(operation);
    
        TreeMap<Date, String> resultMap = timebuilder.buildStatusTimeline(timelist, opTruthTable);
        int flipflops = timebuilder.calcFlipFlops(resultMap);

        TimelineTrends serviceFlipFlop = new TimelineTrends(group, function, resultMap, flipflops);
        out.collect(serviceFlipFlop);

    }

}
