package ops;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class CAggregator {
	
	private CTimeline output;
	private Map<String,CTimeline> inputs;
	
	public CAggregator(String timestamp) throws ParseException
	{
		this.output = new CTimeline(timestamp);
		this.inputs = new HashMap<String,CTimeline>();
	}
	
	public CAggregator(){
		this.output = new CTimeline();
		this.inputs = new HashMap<String,CTimeline>();
		
	}
	
	public void clear(){
		this.output.clear();
		this.inputs.clear();
	}
	
	public String tsFromDate(String date){
		DateTime tmp_date = new DateTime();
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
		tmp_date = fmt.parseDateTime(date);
        tmp_date = tmp_date.withTime(0, 0, 0, 0);
        return tmp_date.toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
	}
	
	public void createTimeline(String name, String timestamp, int prevState){
		CTimeline temp = new CTimeline(timestamp,prevState);
	    this.inputs.put(name, temp);
	}
	
	public void insert(String name, String timestamp, int status){
		// Check if timeline exists, if not create it
		if (this.inputs.containsKey(name) == false)
		{
			CTimeline temp = new CTimeline(timestamp,status);
			this.inputs.put(name, temp);
			return;
		}
		
		this.inputs.get(name).insert(timestamp, status);
	}
	
	public void setFirst(String name, String timestamp, int status){
		// Check if timeline exists, if not create it
		if (this.inputs.containsKey(name) == false)
		{
			CTimeline temp = new CTimeline(timestamp,status);
			this.inputs.put(name, temp);
			return;
		}
		
		this.inputs.get(name).setFirst(timestamp, status);
	}

	public LocalDate getDate(){
		return output.getDate();
	}
	
	public Set<Entry<DateTime, Integer>> getSamples(){
		return this.output.getSamples();
	}
	
	
	public void clearAndSetDate(String timestamp) 
	{
		this.output = new CTimeline(timestamp);
		this.inputs.clear();
		
	}
	
	public void aggregate(OpsManager opsMgr, String op){
		this.output.clear();
		
		//Iterate through all available input timelines and aggregate
		for (CTimeline item : this.inputs.values()) {
			this.output.aggregate(item, opsMgr, opsMgr.getIntOperation(op));
		}
		
	}
}
