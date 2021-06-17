package argo.functions.calctimelines;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import argo.pojos.EndpointTrends;
import argo.profiles.AggregationProfileManager;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StatusFilter, filters data by status
 */
public class ServiceFilter implements FilterFunction<EndpointTrends> {

    static Logger LOG = LoggerFactory.getLogger(ServiceFilter.class);

    //private AggregationProfileParser aggregationProfileParser;
    private AggregationProfileManager aggregationProfileParser;
  
    public ServiceFilter(AggregationProfileManager aggregationProfileParser) {
        this.aggregationProfileParser = aggregationProfileParser;
       
    }

    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(EndpointTrends t) throws Exception {
//       boolean exist= aggregationProfileParser.checkServiceExistance(t.getService());
//        System.out.println("service : "+t.getService()+"  exist: "+exist);
//       
       
      // return exist;
        return aggregationProfileParser.checkServiceExistance(t.getService());
        
    }

}
