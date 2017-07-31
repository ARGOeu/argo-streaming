package sync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class EndpointGroups {

	private static final Logger LOG = Logger.getLogger(EndpointGroups.class.getName());

	private ArrayList<EndpointItem> list;
	private ArrayList<EndpointItem> fList;

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

	public EndpointGroups() {
		this.list = new ArrayList<EndpointItem>();
		this.fList = new ArrayList<EndpointItem>();

	}

	public Iterator<EndpointItem> getIterator() {
		return this.list.iterator();
	}
	
	public int insert(String type, String group, String service, String hostname, HashMap<String, String> tags) {
		EndpointItem new_item = new EndpointItem(type, group, service, hostname, tags);
		this.list.add(new_item);
		return 0; // All good
	}

	public boolean checkEndpoint(String hostname, String service) {

		for (EndpointItem item : fList) {
			if (item.hostname.equals(hostname) && item.service.equals(service)) {
				return true;
			}
		}

		return false;
	}

	public ArrayList<String> getGroup(String type, String hostname, String service) {
		
		ArrayList<String> results = new ArrayList<String>();
		
		for (EndpointItem item : fList) {
			if (item.type.equals(type) && item.hostname.equals(hostname) && item.service.equals(service)) {
				results.add(item.group);
			}
		}

		return results;
	}

	public HashMap<String, String> getGroupTags(String type, String hostname, String service) {

		for (EndpointItem item : fList) {
			if (item.type.equals(type) && item.hostname.equals(hostname) && item.service.equals(service)) {
				return item.tags;
			}
		}

		return null;
	}

	public int count() {
		return this.fList.size();
	}

	public void unfilter() {
		this.fList.clear();
		for (EndpointItem item : this.list) {
			this.fList.add(item);
		}
	}

	public void filter(TreeMap<String, String> fTags) {
		this.fList.clear();
		boolean trim;
		for (EndpointItem item : this.list) {
			trim = false;
			HashMap<String, String> itemTags = item.tags;
			for (Entry<String, String> fTagItem : fTags.entrySet()) {

				if (itemTags.containsKey(fTagItem.getKey())) {
					// First Check binary tags as Y/N 0/1

					if (fTagItem.getValue().equalsIgnoreCase("y") || fTagItem.getValue().equalsIgnoreCase("n")) {
						String binValue = "";
						if (fTagItem.getValue().equalsIgnoreCase("y"))
							binValue = "1";
						if (fTagItem.getValue().equalsIgnoreCase("n"))
							binValue = "0";

						if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(binValue) == false) {
							trim = true;
						}
					} else if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(fTagItem.getValue()) == false) {
						trim = true;
					}

				}
			}

			if (trim == false) {
				fList.add(item);
			}
		}
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

			} // end of avro rows

			this.unfilter();

		} catch (IOException ex) {
			LOG.error("Could not open avro file:" + avroFile.getName());
			throw ex;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(dataFileReader);
		}

	}

}
