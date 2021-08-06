/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.filter.zero.flipflops;

import argo.pojos.GroupTrends;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZeroGroupTrendsFilter, filters GroupTrends data and rejects the ones with flipflop=0
 */
    
    
    public class ZeroGroupTrendsFilter implements FilterFunction<GroupTrends> {

    static Logger LOG = LoggerFactory.getLogger(ZeroGroupTrendsFilter.class);

   
    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(GroupTrends t) throws Exception {
        if (t.getFlipflops()>0) {
            return true;
        }
        return false;
    }

    
}
