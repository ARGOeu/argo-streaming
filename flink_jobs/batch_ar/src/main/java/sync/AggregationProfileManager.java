package sync;



import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

public class AggregationProfileManager {

	private HashMap<String, AvProfileItem> list;
	private static final Logger LOG = Logger.getLogger(AggregationProfileManager.class.getName());

	public AggregationProfileManager() {

		this.list = new HashMap<String, AvProfileItem>();

	}

	private class AvProfileItem {

		private String name;
		private String namespace;
		private String metricProfile;
		private String metricOp;
		private String groupType;
		private String op;

		private HashMap<String, ServGroupItem> groups;
		private HashMap<String, String> serviceIndex;

		AvProfileItem() {
			this.groups = new HashMap<String, ServGroupItem>();
			this.serviceIndex = new HashMap<String, String>();
		}

		private class ServGroupItem {

			String op;
			HashMap<String, String> services;

			ServGroupItem(String op) {
				this.op = op;
				this.services = new HashMap<String, String>();
			}
		}

		// ServGroupItem Declaration Ends Here

		public void insertGroup(String group, String op) {
			if (!this.groups.containsKey(group)) {
				this.groups.put(group, new ServGroupItem(op));
			}
		}

		public void insertService(String group, String service, String op) {
			if (this.groups.containsKey(group)) {
				this.groups.get(group).services.put(service, op);
				this.serviceIndex.put(service, group);
			}
		}
	}

	// AvProfileItem Declaration Ends Here

	public void clearProfiles() {
		this.list.clear();
	}

	public String getTotalOp(String avProfile) {
		if (this.list.containsKey(avProfile)) {
			return this.list.get(avProfile).op;
		}

		return "";
	}

	public String getMetricOp(String avProfile) {
		if (this.list.containsKey(avProfile)) {
			return this.list.get(avProfile).metricOp;
		}

		return "";
	}

	// Return the available Group Names of a profile
	public ArrayList<String> getProfileGroups(String avProfile) {

		if (this.list.containsKey(avProfile)) {
			ArrayList<String> result = new ArrayList<String>();
			Iterator<String> groupIterator = this.list.get(avProfile).groups.keySet().iterator();

			while (groupIterator.hasNext()) {
				result.add(groupIterator.next());
			}

			return result;
		}

		return null;
	}

	// Return the available group operation
	public String getProfileGroupOp(String avProfile, String groupName) {
		if (this.list.containsKey(avProfile)) {
			if (this.list.get(avProfile).groups.containsKey(groupName)) {
				return this.list.get(avProfile).groups.get(groupName).op;
			}
		}

		return null;
	}

	public ArrayList<String> getProfileGroupServices(String avProfile, String groupName) {
		if (this.list.containsKey(avProfile)) {
			if (this.list.get(avProfile).groups.containsKey(groupName)) {
				ArrayList<String> result = new ArrayList<String>();
				Iterator<String> srvIterator = this.list.get(avProfile).groups.get(groupName).services.keySet()
						.iterator();

				while (srvIterator.hasNext()) {
					result.add(srvIterator.next());
				}

				return result;
			}
		}

		return null;
	}

	public String getProfileGroupServiceOp(String avProfile, String groupName, String service) {
		
		if (this.list.containsKey(avProfile)) {
			if (this.list.get(avProfile).groups.containsKey(groupName)) {
				if (this.list.get(avProfile).groups.get(groupName).services.containsKey(service)) {
					return this.list.get(avProfile).groups.get(groupName).services.get(service);
				}
			}
		}

		return null;
	}

	public ArrayList<String> getAvProfiles() {

		if (this.list.size() > 0) {
			ArrayList<String> result = new ArrayList<String>();
			Iterator<String> avpIterator = this.list.keySet().iterator();
			while (avpIterator.hasNext()) {
				result.add(avpIterator.next());
			}

			return result;

		}

		return null;
	}

	public String getProfileNamespace(String avProfile) {

		if (this.list.containsKey(avProfile)) {
			return this.list.get(avProfile).namespace;
		}

		return null;
	}

	public String getProfileMetricProfile(String avProfile) {

		if (this.list.containsKey(avProfile)) {
			return this.list.get(avProfile).metricProfile;
		}

		return null;
	}

	public String getProfileGroupType(String avProfile) {

		if (this.list.containsKey(avProfile)) {
			return this.list.get(avProfile).groupType;
		}

		return null;
	}

	public String getGroupByService(String avProfile, String service) {

		if (this.list.containsKey(avProfile)) {

			return this.list.get(avProfile).serviceIndex.get(service);

		}
		return null;

	}

	public boolean checkService(String avProfile, String service) {

		if (this.list.containsKey(avProfile)) {

			if (this.list.get(avProfile).serviceIndex.containsKey(service)) {
				return true;
			}

		}
		return false;

	}

	public void loadJson(File jsonFile) throws IOException {

		BufferedReader br = null;
		try {

			br = new BufferedReader(new FileReader(jsonFile));

			JsonParser jsonParser = new JsonParser();
			JsonElement jRootElement = jsonParser.parse(br);
			JsonObject jRootObj = jRootElement.getAsJsonObject();

			JsonObject apGroups = jRootObj.getAsJsonObject("groups");

			// Create new entry for this availability profile
			AvProfileItem tmpAvp = new AvProfileItem();

			tmpAvp.name = jRootObj.get("name").getAsString();
			tmpAvp.namespace = jRootObj.get("namespace").getAsString();
			tmpAvp.metricProfile = jRootObj.get("metric_profile").getAsString();
			tmpAvp.metricOp = jRootObj.get("metric_ops").getAsString();
			tmpAvp.groupType = jRootObj.get("group_type").getAsString();
			tmpAvp.op = jRootObj.get("operation").getAsString();

			for (Entry<String, JsonElement> item : apGroups.entrySet()) {
				// service name
				String itemName = item.getKey();
				JsonObject itemObj = item.getValue().getAsJsonObject();
				String itemOp = itemObj.get("operation").getAsString();
				JsonObject itemServices = itemObj.get("services").getAsJsonObject();
				tmpAvp.insertGroup(itemName, itemOp);

				for (Entry<String, JsonElement> subItem : itemServices.entrySet()) {
					tmpAvp.insertService(itemName, subItem.getKey(), subItem.getValue().getAsString());
				}

			}

			// Add profile to the list
			this.list.put(tmpAvp.name, tmpAvp);

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
	
	public void loadJsonString(List<String> apsJson) throws IOException {

		
		try {

			

			JsonParser jsonParser = new JsonParser();
			JsonElement jRootElement = jsonParser.parse(apsJson.get(0));
			JsonObject jRootObj = jRootElement.getAsJsonObject();

			JsonObject apGroups = jRootObj.getAsJsonObject("groups");

			// Create new entry for this availability profile
			AvProfileItem tmpAvp = new AvProfileItem();

			tmpAvp.name = jRootObj.get("name").getAsString();
			tmpAvp.namespace = jRootObj.get("namespace").getAsString();
			tmpAvp.metricProfile = jRootObj.get("metric_profile").getAsString();
			tmpAvp.metricOp = jRootObj.get("metric_ops").getAsString();
			tmpAvp.groupType = jRootObj.get("group_type").getAsString();
			tmpAvp.op = jRootObj.get("operation").getAsString();

			for (Entry<String, JsonElement> item : apGroups.entrySet()) {
				// service name
				String itemName = item.getKey();
				JsonObject itemObj = item.getValue().getAsJsonObject();
				String itemOp = itemObj.get("operation").getAsString();
				JsonObject itemServices = itemObj.get("services").getAsJsonObject();
				tmpAvp.insertGroup(itemName, itemOp);

				for (Entry<String, JsonElement> subItem : itemServices.entrySet()) {
					tmpAvp.insertService(itemName, subItem.getKey(), subItem.getValue().getAsString());
				}

			}

			// Add profile to the list
			this.list.put(tmpAvp.name, tmpAvp);

		 

		} catch (JsonParseException ex) {
			LOG.error("Contents are not valid json");
			throw ex;
		} 

	}
	
	
	

}

