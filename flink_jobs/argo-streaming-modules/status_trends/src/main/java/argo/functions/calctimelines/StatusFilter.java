/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.calctimelines;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 *
 * StatusFilter, filters data by status
 */
public class StatusFilter implements FilterFunction<Tuple6<String, String, String, String, String, Integer>> {

    static Logger LOG = LoggerFactory.getLogger(StatusFilter.class);
    private String status;

    public StatusFilter(String status) {
        this.status = status;
    }
    //if the status field value in Tuple equals the given status returns true, else returns false

    @Override
    public boolean filter(Tuple6<String, String, String, String, String, Integer> t) throws Exception {

        if (t.f4.toString().equalsIgnoreCase(status)) {
            return true;
        }
        return false;
    }

}
