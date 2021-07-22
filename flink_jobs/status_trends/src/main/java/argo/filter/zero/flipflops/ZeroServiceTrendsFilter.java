/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.filter.zero.flipflops;

import argo.functions.calctimelines.ServiceFilter;
import argo.pojos.ServiceTrends;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 */
    public class ZeroServiceTrendsFilter implements FilterFunction<ServiceTrends> {
    
    static Logger LOG = LoggerFactory.getLogger(ZeroServiceTrendsFilter.class);

   
    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(ServiceTrends t) throws Exception {
        if (t.getFlipflops()>0) {
            return true;
        }
        return false;
    }
    
}
