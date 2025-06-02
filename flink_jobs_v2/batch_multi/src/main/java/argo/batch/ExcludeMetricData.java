package argo.batch;

import java.io.IOException;
import java.text.ParseException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import argo.avro.MetricData;
import profilesmanager.RecomputationsManager;
/**
 * Receives MetricData and filters them by excluding monitoring engine based on recomputation information
 * retrieved by broadcast variable "rec" and handled by an internal recomputation manager 
 */
public class ExcludeMetricData extends RichFlatMapFunction<MetricData,MetricData> {

	private static final long serialVersionUID = 1L;

	
	public ExcludeMetricData(){
	}
	
	static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);
	
	private List<Map<String, ArrayList<Map<String, Date>>>> rec;

	@Override
	public void open(Configuration parameters) throws IOException, ParseException {
		// Get recomputation data from broadcast variable

		this.rec = getRuntimeContext().getBroadcastVariable("rec");
        RecomputationsManager.monEngines=this.rec.get(0);
	}

	@Override
	public void flatMap(MetricData md, Collector<MetricData> out) throws Exception {

		// Get monitoring host from input metric data
		String monHost = md.getMonitoringHost();
		// Get timestamp from input metric data
		String ts = md.getTimestamp();

		// Check if monitoring host and metric data coincide with exclusions by monitoring
		// engine in the current available recomputations
		if (RecomputationsManager.isMonExcluded(monHost, ts)) return;
		
		// if not excluded collect the result in the output
		out.collect(md);
			
		
	}
}