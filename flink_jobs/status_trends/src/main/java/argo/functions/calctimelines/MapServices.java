package argo.functions.calctimelines;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.ServiceTrends;
import argo.profiles.AggregationProfileParser;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author cthermolia
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ARGO-2985 Calculate flip flops for group of endpoints
=======
>>>>>>> ARGO-2985 Calculate flip flops for group of endpoints
=======
>>>>>>> ARGO-2985 Calculate flip flops for group of endpoints
 * MapServices produces TimelineTrends for each service,that maps to the groups
 * of functions as described in aggregation profile groups endpoint , metric
 */
public class MapServices implements FlatMapFunction<ServiceTrends, ServiceTrends> {

    private AggregationProfileParser aggregationProfileParser;
    public MapServices(AggregationProfileParser aggregationProfileParser) {
        this.aggregationProfileParser = aggregationProfileParser;
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
    public void flatMap(ServiceTrends t, Collector<ServiceTrends> out) throws Exception {
        String service = t.getService();

        ArrayList<String> functionList =aggregationProfileParser.retrieveServiceFunctions(service);
        if (functionList != null) {
            for (String f : functionList) {
                ServiceTrends newT = t;
                newT.setFunction(f);
                out.collect(newT);
            }
        }
    }

}
