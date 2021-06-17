package argo.filter.zero.flipflops;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import argo.functions.calctimelines.ServiceFilter;
import argo.pojos.EndpointTrends;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZeroEndpointTrendsFilter, filters EndpointTrends data and rejects the ones with flipflop=0
 */
public class ZeroEndpointTrendsFilter implements FilterFunction<EndpointTrends> {

    static Logger LOG = LoggerFactory.getLogger(ServiceFilter.class);

   
    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(EndpointTrends t) throws Exception {
        if (t.getFlipflops()>0) {
            return true;
        }
        return false;
    }

}
