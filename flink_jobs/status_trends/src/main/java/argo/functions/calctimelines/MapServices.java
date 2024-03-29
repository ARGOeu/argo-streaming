package argo.functions.calctimelines;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import argo.pojos.ServiceTrends;
import argo.profiles.AggregationProfileManager;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * MapServices produces TimelineTrends for each service,that maps to the groups
 * of functions as described in aggregation profile groups endpoint , metric
 */


public class MapServices implements FlatMapFunction<ServiceTrends, ServiceTrends> {

    //private AggregationProfileParser aggregationProfileParser;
    private AggregationProfileManager aggregationProfileParser;
   
    private HashMap<String, ArrayList<String>> serviceFunctions;

    public MapServices(AggregationProfileManager aggregationProfileParser) {
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

        String function = aggregationProfileParser.retrieveFunctionsByService(service);
      
           
                ServiceTrends newT = t;
                newT.setFunction(function);
                out.collect(newT);
           
        
    }
}
