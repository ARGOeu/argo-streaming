/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.filter.zero.flipflops;

import argo.pojos.GroupFunctionTrends;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZeroGroupFunctionFilter, filters GroupFunctionTrends data and rejects the ones with flipflop=0
 */
    
    public class ZeroGroupFunctionFilter implements FilterFunction<GroupFunctionTrends> {

    static Logger LOG = LoggerFactory.getLogger(ZeroGroupFunctionFilter.class);

   
    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(GroupFunctionTrends t) throws Exception {
        if (t.getFlipflops()>0) {
            return true;
        }
        return false;
    }

}
