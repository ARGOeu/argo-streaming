package ops;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;


public class ConfigManager {

	private static final Logger LOG = Logger.getLogger(ConfigManager.class.getName());

	public String id; // report uuid reference
	public String report;
	public String tenant;
	public String egroup; // endpoint group
	public String ggroup; // group of groups
	public String weight; // weight factor type
	public TreeMap<String, String> egroupTags;
	public TreeMap<String, String> ggroupTags;
	public TreeMap<String, String> mdataTags;

	public ConfigManager() {
		this.report = null;
		this.id = null;
		this.tenant = null;
		this.egroup = null;
		this.ggroup = null;
		this.weight = null;
		this.egroupTags = new TreeMap<String, String>();
		this.ggroupTags = new TreeMap<String, String>();
		this.mdataTags = new TreeMap<String, String>();

	}

	public void clear() {
		this.id = null;
		this.report = null;
		this.tenant = null;
		this.egroup = null;
		this.ggroup = null;
		this.weight = null;
		this.egroupTags.clear();
		this.ggroupTags.clear();
		this.mdataTags.clear();

	}
	
	public String getReportID() {
		return id;
	}
	
	public String getReport() {
		return report;
	}
	
	public String getTenant() {
		return tenant;
	}
	
	
	public String getEgroup() {
		return egroup;
	}

	public void loadJson(File jsonFile) throws IOException {
		// Clear data
		this.clear();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(jsonFile));

			JsonParser jsonParser = new JsonParser();
			JsonElement jElement = jsonParser.parse(br);
			JsonObject jObj = jElement.getAsJsonObject();
			// Get the simple fields
			this.id = jObj.get("id").getAsString();
			this.tenant = jObj.get("tenant").getAsString();
			this.report = jObj.get("info").getAsJsonObject().get("name").getAsString();
			
			// get topology schema names
			JsonObject topoGroup = jObj.get("topology_schema").getAsJsonObject().getAsJsonObject("group");
			this.ggroup = topoGroup.get("type").getAsString();
			this.egroup = topoGroup.get("group").getAsJsonObject().get("type").getAsString();
			
			// optional weight filtering
			this.weight = "";
			if (jObj.has("weight")){
				this.weight = jObj.get("weight").getAsString();
			}
			// Get compound fields
			JsonArray jTags = jObj.getAsJsonArray("filter_tags");
			
			// Iterate tags
			if (jTags != null) {
				for (JsonElement tag : jTags) {
					JsonObject jTag = tag.getAsJsonObject();
					String name = jTag.get("name").getAsString();
					String value = jTag.get("value").getAsString();
					String ctx = jTag.get("context").getAsString();
					if (ctx.equalsIgnoreCase("group_of_groups")){
						this.ggroupTags.put(name, value);
					} else if (ctx.equalsIgnoreCase("endpoint_groups")){
						this.egroupTags.put(name, value);
					} else if (ctx.equalsIgnoreCase("metric_data")) {
						this.mdataTags.put(name, value);
					}
					
				}
			}
			

		} catch (FileNotFoundException ex) {
			LOG.error("Could not open file:" + jsonFile.getName());
			throw ex;

		} catch (JsonParseException ex) {
			LOG.error("File is not valid json:" + jsonFile.getName());
			throw ex;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(br);
		}

	}

	
	/**
	 * Loads Report config information from a config json string
	 * 
	 */
	public void loadJsonString(List<String> confJson) throws JsonParseException {
		// Clear data
		this.clear();

		try {

			JsonParser jsonParser = new JsonParser();
			// Grab the first - and only line of json from ops data
			JsonElement jElement = jsonParser.parse(confJson.get(0));
			JsonObject jObj = jElement.getAsJsonObject();
			// Get the simple fields
			this.id = jObj.get("id").getAsString();
			this.tenant = jObj.get("tenant").getAsString();
			this.report = jObj.get("info").getAsJsonObject().get("name").getAsString();
			// get topology schema names
			JsonObject topoGroup = jObj.get("topology_schema").getAsJsonObject().getAsJsonObject("group");
			this.ggroup = topoGroup.get("type").getAsString();
			this.egroup = topoGroup.get("group").getAsJsonObject().get("type").getAsString();
			// optional weight filtering
			this.weight = "";
			if (jObj.has("weight")){
				this.weight = jObj.get("weight").getAsString();
			}
			// Get compound fields
			JsonArray jTags = jObj.getAsJsonArray("tags");
			
			// Iterate tags
			if (jTags != null) {
				for (JsonElement tag : jTags) {
					JsonObject jTag = tag.getAsJsonObject();
					String name = jTag.get("name").getAsString();
					String value = jTag.get("value").getAsString();
					String ctx = jTag.get("context").getAsString();
					if (ctx.equalsIgnoreCase("group_of_groups")){
						this.ggroupTags.put(name, value);
					} else if (ctx.equalsIgnoreCase("endpoint_groups")){
						this.egroupTags.put(name, value);
					} else if (ctx.equalsIgnoreCase("metric_data")) {
						this.mdataTags.put(name, value);
					}
					
				}
			}

		} catch (JsonParseException ex) {
			LOG.error("Not valid json contents");
			throw ex;
		} 

	}

}
