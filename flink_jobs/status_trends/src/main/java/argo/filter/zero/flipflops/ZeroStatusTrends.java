/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.filter.zero.flipflops;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZeroStatusTrends, filters Status Trends data and rejects the ones with flipflop=0
 */
    public class ZeroStatusTrends implements FilterFunction<Tuple6<String, String, String, String, String, Integer>> {
    
    static Logger LOG = LoggerFactory.getLogger(ZeroStatusTrends.class);

   
    //if the status field value in Tuple equals the given status returns true, else returns false
    @Override
    public boolean filter(Tuple6<String, String, String, String, String, Integer> t) throws Exception {
        if (t.f5>0) {
            return true;
        }
        return false;
    }
    
}
