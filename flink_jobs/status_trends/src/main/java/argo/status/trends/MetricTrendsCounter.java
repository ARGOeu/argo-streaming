package argo.status.trends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.MetricTrends;
import argo.profiles.OperationsParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

/**
 * MetricTrendsCounter calculates on the dataset's timeline the num of
 * appearances of the status CRITICAL, WARNING,UNKNOWN and produces a dataset of
 * tuples that contain these calculations
 */
public class MetricTrendsCounter implements FlatMapFunction<MetricTrends, Tuple6<String, String, String, String, String, Integer>> {

    private OperationsParser operationsParser;

    public MetricTrendsCounter(OperationsParser operationsParser) {
        this.operationsParser = operationsParser;
    }

    /**
     * if the service exist in one or more function groups , timeline trends are
     * produced for each function that the service belongs and the function info
     * is added to the timelinetrend
     *
     * @param t
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(MetricTrends t, Collector< Tuple6<String, String, String, String, String, Integer>> out) throws Exception {

        Tuple6<String, String, String, String, String, Integer> tupleCritical = new Tuple6<String, String, String, String, String, Integer>(
                t.getGroup(), t.getService(), t.getEndpoint(), t.getMetric(), "CRITICAL", t.getCriticalNum());
        out.collect(tupleCritical);

        Tuple6<String, String, String, String, String, Integer> tupleWarning = new Tuple6<String, String, String, String, String, Integer>(
                t.getGroup(), t.getService(), t.getEndpoint(), t.getMetric(), "WARNING", t.getWarningNum());

        out.collect(tupleWarning);

        Tuple6<String, String, String, String, String, Integer> tupleUnknown = new Tuple6<String, String, String, String, String, Integer>(
                t.getGroup(), t.getService(), t.getEndpoint(), t.getMetric(), "UNKNWON", t.getUnknownNum());
        out.collect(tupleUnknown);

    }
}
