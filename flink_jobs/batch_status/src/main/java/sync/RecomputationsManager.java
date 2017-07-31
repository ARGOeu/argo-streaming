package sync;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RecomputationsManager {

	private static final Logger LOG = Logger.getLogger(RecomputationsManager.class.getName());

	public Map<String,ArrayList<Map<String,String>>> groups;
	// Recomputations for filtering monitoring engine results
	public Map<String,ArrayList<Map<String,Date>>> monEngines; 
	
	public RecomputationsManager() {
		this.groups = new HashMap<String,ArrayList<Map<String,String>>>();
		this.monEngines = new HashMap<String,ArrayList<Map<String,Date>>>();
	}

	// Clear all the recomputation data
	public void clear() {
		this.groups = new HashMap<String,ArrayList<Map<String,String>>>();
		this.monEngines = new HashMap<String,ArrayList<Map<String,Date>>>();
	}
	
	// Insert new recomputation data for a specific endpoint group
	public void insert(String group, String start, String end) {
		
		Map<String,String>temp = new HashMap<String,String>();
		temp.put("start", start);
		temp.put("end",end);
		
		if (this.groups.containsKey(group) == false){
			this.groups.put(group, new ArrayList<Map<String,String>>());
		} 
		
		this.groups.get(group).add(temp);
		
	}
	
	// Insert new recomputation data for a specific monitoring engine
	public void insertMon(String monHost, String start, String end) throws ParseException {
		
		Map<String,Date>temp = new HashMap<String,Date>();
		SimpleDateFormat tsW3C = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		temp.put("s", tsW3C.parse(start));
		temp.put("e",tsW3C.parse(end));
		
		if (this.monEngines.containsKey(monHost) == false){
			this.monEngines.put(monHost, new ArrayList<Map<String,Date>>());
		} 
		
		this.monEngines.get(monHost).add(temp);
		
	}

	// Check if group is excluded in recomputations
	public boolean isExcluded (String group){
		return this.groups.containsKey(group);
	}
	
	
	
	// Check if a recomputation period is valid for target date
	public boolean validPeriod(String target, String start, String end) throws ParseException {

		SimpleDateFormat dmy = new SimpleDateFormat("yyyy-MM-dd");
		Date tDate = dmy.parse(target);
		Date sDate = dmy.parse(start);
		Date eDate = dmy.parse(end);

		return (tDate.compareTo(sDate) >= 0 && tDate.compareTo(eDate) <= 0);

	}

	public ArrayList<Map<String,String>> getPeriods(String group,String targetDate) throws ParseException {
		ArrayList<Map<String,String>> periods = new ArrayList<Map<String,String>>();
		
		if (this.groups.containsKey(group)){
			for (Map<String,String> period : this.groups.get(group)){
				if (this.validPeriod(targetDate, period.get("start"), period.get("end"))){
					periods.add(period);
				}
			}
			
		}
		
		return periods;
	}
	
	// 
	public boolean isMonExcluded(String monHost, String inputTs) throws ParseException{
		
		if (this.monEngines.containsKey(monHost) == false)
		{
			return false;
		}
		SimpleDateFormat tsW3C = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date targetDate = tsW3C.parse(inputTs);
		for (Map<String, Date> item : this.monEngines.get(monHost))
		{
		
			if  (!(targetDate.before(item.get("s")) || targetDate.after(item.get("e")))) {
				return true;
			}
		}
		
		return false;
	}

	

	public void loadJson(File jsonFile) throws IOException, ParseException {

		this.clear();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(jsonFile));

			JsonParser jsonParser = new JsonParser();
			JsonElement jRootElement = jsonParser.parse(br);
			JsonArray jRootObj = jRootElement.getAsJsonArray();

			for (JsonElement item : jRootObj) {
				
				// Get the excluded sites 
				if (item.getAsJsonObject().get("start_time") != null  
						&& item.getAsJsonObject().get("end_time") != null
						&& item.getAsJsonObject().get("exclude") != null )  {
				
					String start = item.getAsJsonObject().get("start_time").getAsString();
					String end = item.getAsJsonObject().get("end_time").getAsString();
		
					// Get the excluded
					JsonArray jExclude = item.getAsJsonObject().get("exclude").getAsJsonArray();
					for (JsonElement subitem : jExclude) {
						this.insert(subitem.getAsString(),start,end);
					}
				}
				
				// Get the excluded Monitoring sources
				if (item.getAsJsonObject().get("exclude_monitoring_source") != null) {
					JsonArray jMon = item.getAsJsonObject().get("exclude_monitoring_source").getAsJsonArray();
					for (JsonElement subitem: jMon){
						
						String monHost = subitem.getAsJsonObject().get("host").getAsString();
						String monStart = subitem.getAsJsonObject().get("start_time").getAsString();
						String monEnd = subitem.getAsJsonObject().get("end_time").getAsString();
						this.insertMon(monHost, monStart, monEnd);
					}
				}

			}

		} catch (FileNotFoundException ex) {
			LOG.error("Could not open file:" + jsonFile.getName());
			throw ex;

		} catch (ParseException pex) {
			LOG.error("Parsing date error");
			throw pex;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(br);
		}

	}
	
	/**
	 * Load Recompuatation information from a JSON string instead of a File source. 
	 * This method is used in execution enviroments where the required data is provided by broadcast variables
	 */
	public void loadJsonString(List<String> recJson) throws IOException, ParseException {

		this.clear();

		
		try {
			

			JsonParser jsonParser = new JsonParser();
			JsonElement jRootElement = jsonParser.parse(recJson.get(0));
			JsonArray jRootObj = jRootElement.getAsJsonArray();

			for (JsonElement item : jRootObj) {
				
				// Get the excluded sites 
				if (item.getAsJsonObject().get("start_time") != null  
						&& item.getAsJsonObject().get("end_time") != null
						&& item.getAsJsonObject().get("exclude") != null )  {
				
					String start = item.getAsJsonObject().get("start_time").getAsString();
					String end = item.getAsJsonObject().get("end_time").getAsString();
		
					// Get the excluded
					JsonArray jExclude = item.getAsJsonObject().get("exclude").getAsJsonArray();
					for (JsonElement subitem : jExclude) {
						this.insert(subitem.getAsString(),start,end);
					}
				}
				
				// Get the excluded Monitoring sources
				if (item.getAsJsonObject().get("exclude_monitoring_source") != null) {
					JsonArray jMon = item.getAsJsonObject().get("exclude_monitoring_source").getAsJsonArray();
					for (JsonElement subitem: jMon){
						
						String monHost = subitem.getAsJsonObject().get("host").getAsString();
						String monStart = subitem.getAsJsonObject().get("start_time").getAsString();
						String monEnd = subitem.getAsJsonObject().get("end_time").getAsString();
						this.insertMon(monHost, monStart, monEnd);
					}
				}

			}

		

		} catch (ParseException pex) {
			LOG.error("Parsing date error");
			throw pex;
		}
	}
	

}
