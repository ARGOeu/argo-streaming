package argo.functions.timeline;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import argo.avro.MetricData;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 * CalcLastTimeStatus keeps data of the latest time entry
*/

public class CalcLastTimeStatus implements GroupReduceFunction<MetricData, MetricData> {

 /**
 * 
 * @param in, the initial dataset of the MetricData
 * @param out , the output dataset containing the MetricData of the latest timestamp
 * @throws Exception 
 */
    @Override
    public void reduce(Iterable<MetricData> in, Collector<MetricData> out) throws Exception {
        TreeMap<String, MetricData> timeStatusMap = new TreeMap<>();
        for (MetricData md : in) {
            timeStatusMap.put(md.getTimestamp().toString(), md);
            
        }
        out.collect(timeStatusMap.lastEntry().getValue());
    }
}