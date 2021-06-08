package timelines;



import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import ops.OpsManager;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
public class TimelineAggregator {
	
	private Timeline output;
	private Map<String,Timeline> inputs;
	
	public TimelineAggregator(String timestamp) throws ParseException
	{
		this.output = new Timeline();
		this.inputs = new HashMap<String,Timeline>();
	}
	
	public TimelineAggregator(){
		this.output = new Timeline();
		this.inputs = new HashMap<String,Timeline>();
		
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
		Timeline temp = new Timeline();
                temp.insert(timestamp, prevState);
	    this.inputs.put(name, temp);
	}
	
	public void insert(String name, String timestamp, int status){
		// Check if timeline exists, if not create it
		if (this.inputs.containsKey(name) == false)
		{
			Timeline temp = new Timeline();
                        temp.insert(timestamp, status);
			this.inputs.put(name, temp);
			return;
		}
		
		this.inputs.get(name).insert(timestamp, status);
	}
	
	public void setFirst(String name, String timestamp, int status){
		// Check if timeline exists, if not create it
		if (this.inputs.containsKey(name) == false)
		{
			Timeline temp = new Timeline();
                        temp.insert(timestamp, status);
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
		this.output = new Timeline(timestamp);
		this.inputs.clear();
		
	}

    public Timeline getOutput() {
        return output;
    }

    public void setOutput(Timeline output) {
        this.output = output;
    }

    public Map<String, Timeline> getInputs() {
        return inputs;
    }

    public void setInputs(Map<String, Timeline> inputs) {
        this.inputs = inputs;
    }
	
     public void aggregate(OpsManager opsMgr, String op){
     
       TimelineMerger.aggregate((ArrayList<Timeline>)this.inputs.values(), opsMgr.getTruthTable(), opsMgr.getIntOperation(op));
       this.output=TimelineMerger.getOutput();
     }

}
