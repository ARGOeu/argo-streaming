package argo.status.trends;



/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.ServiceTrends;
import argo.profiles.OperationsParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import timelines.Timeline;


/**
 * ServiceTrendsCounter calculates on the dataset's timeline the num of appearances of the status
 * CRITICAL, WARNING,UNKNOWN and produces a dataset of tuples that contain these calculations
 */
public class ServiceTrendsCounter implements FlatMapFunction<ServiceTrends, Tuple6< String,String, String, String, String, Integer>> {

    private OperationsParser operationsParser;

    public ServiceTrendsCounter(OperationsParser operationsParser) {
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
    public void flatMap(ServiceTrends t, Collector< Tuple6<String, String, String, String, String, Integer>> out) throws Exception {

        int criticalstatus = operationsParser.getIntStatus("CRITICAL");
        int warningstatus = operationsParser.getIntStatus("WARNING");
        int unknownstatus = operationsParser.getIntStatus("UNKNOWN");

        Timeline timeline = t.getTimeline();
        int criticalSum = timeline.countStatusAppearances(criticalstatus);
        int warningSum = timeline.countStatusAppearances(warningstatus);
        int unknownSum = timeline.countStatusAppearances(unknownstatus);

        Tuple6< String,String, String, String, String, Integer> tupleCritical = new Tuple6<  String,String, String, String, String, Integer>(
                t.getGroup(), t.getService(),null,null, "CRITICAL", criticalSum);
        out.collect(tupleCritical);

        Tuple6<  String,String, String, String, String, Integer> tupleWarning = new Tuple6< String, String, String, String, String, Integer>(
                t.getGroup(), t.getService(),null, null,"WARNING", warningSum);
        
        out.collect(tupleWarning);

        Tuple6<  String,String, String, String, String, Integer> tupleUnknown = new Tuple6<  String,String, String, String, String, Integer>(
                t.getGroup(), t.getService(),null,null, "UNKNOWN", unknownSum);
            out.collect(tupleUnknown);
    
    }
}
