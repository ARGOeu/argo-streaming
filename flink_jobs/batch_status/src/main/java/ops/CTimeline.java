package ops;


import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

import argo.batch.ArgoStatusBatch;


public class CTimeline {
	
	private LocalDate date;
	
	static Logger LOG = LoggerFactory.getLogger(CTimeline.class);
	
	private TreeMap<DateTime,Integer> samples;
	
	CTimeline()
	{
		this.date = null;
		this.samples = new TreeMap<DateTime,Integer>();
	}
	
	CTimeline(String timestamp){
		DateTime tmp_date = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		tmp_date = fmt.parseDateTime(timestamp);
		tmp_date.withTime(0, 0, 0, 0);
		this.date = tmp_date.toLocalDate();
		this.samples = new TreeMap<DateTime,Integer>();
	}
	
	CTimeline(String timestamp, int state){
		DateTime tmp_date = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		tmp_date = fmt.parseDateTime(timestamp);
		tmp_date = tmp_date.withTime(0, 0, 0, 0);
		this.date = tmp_date.toLocalDate();
		this.samples = new TreeMap<DateTime,Integer>();
		this.samples.put(tmp_date, state);
		
	}
	
	public int get(String timestamp) {
		DateTime tmp_date = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		tmp_date = fmt.parseDateTime(timestamp);
		return this.samples.floorEntry(tmp_date).getValue();
	}
	
	public int get(DateTime point) {
		if (this.samples.floorEntry(point) == null){
			
			throw new RuntimeException("no item found in timeline, size of timeline:" + this.samples.size() + "," + point.toString());
		}
		return this.samples.floorEntry(point).getValue();
	}
	
	public void insert(String timestamp, int status) 
	{

		DateTime tmp_date = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		tmp_date = fmt.parseDateTime(timestamp);
		this.samples.put(tmp_date, status);
	}
	
	public void insert(DateTime date, int status) 
	{
		samples.put(date, status);
		
	}
	
	public void setFirst(String timestamp, int state)
	{
		DateTime tmp_date = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		tmp_date = fmt.parseDateTime(timestamp);
		this.samples = new TreeMap<DateTime,Integer>();
		tmp_date = tmp_date.withTime(0, 0, 0, 0);
		this.samples.put(tmp_date, state);
	}
	
	public void clear(){
		this.samples.clear();
	}
	
	public void bulkInsert(Set<Entry<DateTime, Integer>> samples){
		this.samples.clear();
		for (Entry<DateTime, Integer> entry : samples){
			this.samples.put(entry.getKey(), entry.getValue());
		}
	}
	
	public Set<Entry<DateTime, Integer>> getSamples(){
		return samples.entrySet();
	}
	
	public LocalDate getDate(){
		return this.date;
	}
	
	public int getLength(){
		return this.samples.size();
	}
	
	public boolean isEmpty() {
		return this.samples.isEmpty();
	}
	
	public void optimize()
	{
		TreeMap<DateTime,Integer> optimal = new TreeMap<DateTime,Integer>();
		int prevstate = -1;
		for (DateTime key : this.samples.keySet()){
			int value = this.samples.get(key);
			if (prevstate == -1) {
				
				optimal.put(key, value);
				prevstate = value;
				
			}
			if (prevstate != value){
				optimal.put(key, value);
				prevstate = value;
			}
		}
		
		this.samples = optimal;
	}
	
	public Set<DateTime> getPoints(){
		return this.samples.keySet();
	}
	
	public void aggregate(CTimeline second, OpsManager opsMgr, int op){
		if (this.isEmpty()){
			this.bulkInsert(second.getSamples());
			// Optimize even when we have a single timeline for aggregation
			this.optimize(); 
			return;
		}
		
		CTimeline result = new CTimeline();
		
		// Slice for first
		for (DateTime point : this.getPoints()){
			result.insert(point, -1);
		}
		// Slice for second 
		for (DateTime point : second.getPoints()){
			result.insert(point, -1);
		}
		
		// Iterate over result and ask
		for (DateTime point : result.getPoints()){
			int a = this.get(point);
			int b = second.get(point);
			int x = opsMgr.opInt(op, a, b);
			result.insert(point, x);
		}
		
		result.optimize();
		
		// Engrave the result in this timeline
		this.clear();
		this.bulkInsert(result.getSamples());
	}
	
}
