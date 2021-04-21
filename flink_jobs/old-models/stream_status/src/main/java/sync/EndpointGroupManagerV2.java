package sync;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import argo.avro.GroupEndpoint;

public class EndpointGroupManagerV2 {

	private static final Logger LOG = Logger.getLogger(EndpointGroupManager.class.getName());

	private Map<String,Map<String,EndpointItem>> list;
	private Map<String,ArrayList<EndpointItem>> groupIndex;
	
	private String defaultType = null;

	public class EndpointItem {
		String type; // type of group
		String group; // name of the group
		String service; // type of the service
		String hostname; // name of host
		HashMap<String, String> tags; // Tag list

		public EndpointItem() {
			// Initializations
			this.type = "";
			this.group = "";
			this.service = "";
			this.hostname = "";
			this.tags = new HashMap<String, String>();
		}

		public EndpointItem(String type, String group, String service, String hostname, HashMap<String, String> tags) {
			this.type = type;
			this.group = group;
			this.service = service;
			this.hostname = hostname;
			this.tags = tags;

		}
		
		public String getType() { return type; }
		public String getGroup() { return group; }
		public String getService() { return service; }
		public String getHostname() { return hostname; }

	}
	
	public Map<String,Map<String,EndpointItem>> getList(){
		return this.list;
	}

	public EndpointGroupManagerV2() {
		this.list = new HashMap<String,Map<String,EndpointItem>>();
		this.groupIndex = new HashMap<String,ArrayList<EndpointItem>>();
	

	}
	

	public int insert(String type, String group, String service, String hostname, HashMap<String, String> tags) {
		EndpointItem itemNew = new EndpointItem(type, group, service, hostname, tags);
		String key = type + "|" + hostname + "|" + service;
		if (!list.containsKey(key)){
			Map<String,EndpointItem> subList = new HashMap<String,EndpointItem>();
			subList.put(group, itemNew);
			list.put(key,subList);

		} else {
			Map<String, EndpointItem> subList = list.get(key);
			subList.put(group, itemNew);
		}
		// Add item to the secondary group index
		if (!groupIndex.containsKey(group)){
			groupIndex.put(group, new ArrayList<EndpointItem>(Arrays.asList(itemNew)));
		} else {
			groupIndex.get(group).add(itemNew);
		}
	    
		return 0; // All good
	}

	public boolean checkEndpoint(String hostname, String service) {

		String key = defaultType + "|" + hostname + "|" + service;
		return list.containsKey(key);
	}

	public ArrayList<String> getGroupFull(String type, String hostname, String service) {
		
		String key = type + "|" + hostname + "|" + service;
		Map<String,EndpointItem> sublist = list.get(key);
		if (sublist != null) {
			return new ArrayList<String>(list.get(key).keySet());
		} 
		
		return new ArrayList<String>();
	
	}
	
	public Iterator<EndpointItem> getGroupIter(String group) {
		ArrayList<EndpointItem> list = groupIndex.get(group);
		if (list!=null){
			return list.iterator();
		}
		
		return null;
	}
	
	public ArrayList<String> getGroup(String hostname, String service) {
		

		String key = defaultType + "|" + hostname + "|" + service;
		Map<String,EndpointItem> sublist = list.get(key);
		if (sublist != null) {
			return new ArrayList<String>(list.get(key).keySet());
		} 
		
		return new ArrayList<String>();
		
	}
        
        public String getTagUrl(String group, String hostname, String service) {
		
		String key = defaultType + "|" + hostname + "|" + service;
		Map<String,EndpointItem> sublist = list.get(key);
                
                EndpointItem item=sublist.get(group);
                String url ="";
                if(item.tags.get("info.URL")!=null){
                    url=item.tags.get("info.URL");
                }
		
		return url;
		
	}


	/**
	 * Loads endpoint grouping information from an avro file
	 * <p>
	 * This method loads endpoint grouping information contained in an .avro
	 * file with specific avro schema.
	 * 
	 * <p>
	 * The following fields are expected to be found in each avro row:
	 * <ol>
	 * <li>type: string (describes the type of grouping)</li>
	 * <li>group: string</li>
	 * <li>service: string</li>
	 * <li>hostname: string</li>
	 * <li>tags: hashmap (contains a map of arbitrary key values)</li>
	 * </ol>
	 * 
	 * @param avroFile
	 *            a File object of the avro file that will be opened
	 * @throws IOException
	 *             if there is an error during opening of the avro file
	 */
	@SuppressWarnings("unchecked")
	public void loadAvro(File avroFile) throws IOException {

		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

			// Grab Avro schema
			Schema avroSchema = dataFileReader.getSchema();

			// Generate 1st level generic record reader (rows)
			GenericRecord avroRow = new GenericData.Record(avroSchema);

			// For all rows in file repeat
			while (dataFileReader.hasNext()) {
				// read the row
				avroRow = dataFileReader.next(avroRow);
				HashMap<String, String> tagMap = new HashMap<String, String>();

				// Generate 2nd level generic record reader (tags)

				HashMap<Utf8, String> tags = (HashMap<Utf8, String>) (avroRow.get("tags"));

				if (tags != null) {
					for (Utf8 item : tags.keySet()) {
						tagMap.put(item.toString(), String.valueOf(tags.get(item)));
					}
				}

				// Grab 1st level mandatory fields
				String type = avroRow.get("type").toString();
				String group = avroRow.get("group").toString();
				String service = avroRow.get("service").toString();
				String hostname = avroRow.get("hostname").toString();

				// Insert data to list
				this.insert(type, group, service, hostname, tagMap);
				defaultType=type;

			} // end of avro rows

			

		} catch (IOException ex) {
			LOG.error("Could not open avro file:" + avroFile.getName());
			throw ex;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(dataFileReader);
		}

	}
	
	
	
	/**
	 * Loads information from a list of EndpointGroup objects
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void loadFromList( List<GroupEndpoint> egp)  {

		// For each endpoint group record
		for (GroupEndpoint item : egp){
			String type = item.getType();
			String group = item.getGroup();
			String service = item.getService();
			String hostname = item.getHostname();
			HashMap<String, String> tagMap = new HashMap<String, String>();
			HashMap<String, String> tags = (HashMap<String, String>) item.getTags();
			
			if (tags != null) {
				for (String key : tags.keySet()) {
					tagMap.put(key, tags.get(key));
				}
			}
			
			// Insert data to list
			this.insert(type, group, service, hostname, tagMap);
			defaultType=type;
		}
		
		
		

	}

}
