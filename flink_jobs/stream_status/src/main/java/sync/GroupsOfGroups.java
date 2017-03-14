package sync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class GroupsOfGroups {

	static Logger log = Logger.getLogger(GroupsOfGroups.class.getName());

	private ArrayList<GroupItem> list;
	private ArrayList<GroupItem> fList;

	private class GroupItem {
		String type; // type of group
		String group; // name of the group
		String subgroup; // name of sub-group
		HashMap<String, String> tags; // Tag list

		public GroupItem() {
			// Initializations
			this.type = "";
			this.group = "";
			this.subgroup = "";
			this.tags = new HashMap<String, String>();
		}

		public GroupItem(String type, String group, String subgroup, HashMap<String, String> tags) {
			this.type = type;
			this.group = group;
			this.subgroup = subgroup;
			this.tags = tags;

		}

	}

	public GroupsOfGroups() {
		this.list = new ArrayList<GroupItem>();
		this.fList = new ArrayList<GroupItem>();
	}

	public int insert(String type, String group, String subgroup, HashMap<String, String> tags) {
		GroupItem new_item = new GroupItem(type, group, subgroup, tags);
		this.list.add(new_item);
		return 0; // All good
	}

	public HashMap<String, String> getGroupTags(String type, String subgroup) {
		for (GroupItem item : this.fList) {
			if (item.type.equals(type) && item.subgroup.equals(subgroup)) {
				return item.tags;
			}
		}

		return null;
	}

	public int count() {
		return this.fList.size();
	}

	public String getGroup(String type, String subgroup) {
		for (GroupItem item : this.fList) {
			if (item.type.equals(type) && item.subgroup.equals(subgroup)) {
				return item.group;
			}
		}

		return null;
	}

	public void unfilter() {
		this.fList.clear();
		for (GroupItem item : this.list) {
			this.fList.add(item);
		}
	}

	public void filter(TreeMap<String, String> fTags) {
		this.fList.clear();
		boolean trim;
		for (GroupItem item : this.list) {
			trim = false;
			HashMap<String, String> itemTags = item.tags;
			for (Entry<String, String> fTagItem : fTags.entrySet()) {

				if (itemTags.containsKey(fTagItem.getKey())) {
					if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(fTagItem.getValue()) == false) {
						trim = true;
					}

				}
			}

			if (trim == false) {
				fList.add(item);
			}
		}
	}

	public boolean checkSubGroup(String subgroup) {
		for (GroupItem item : fList) {
			if (item.subgroup.equals(subgroup)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Loads groups of groups information from an avro file
	 * <p>
	 * This method loads groups of groups information contained in an .avro file
	 * with specific avro schema.
	 * 
	 * <p>
	 * The following fields are expected to be found in each avro row:
	 * <ol>
	 * <li>type: string (describes the type of grouping)</li>
	 * <li>group: string</li>
	 * <li>subgroup: string</li>
	 * <li>tags: hashmap (contains a map of arbitrary key values)</li>
	 * </ol>
	 * 
	 * @param avroFile
	 *            a File object of the avro file that will be opened
	 * @throws IOException
	 *             if there is an error during opening of the avro file
	 */
	public void loadAvro(File avroFile) throws IOException {

		// Prepare Avro File Readers
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

			// Grab avro schema
			Schema avroSchema = dataFileReader.getSchema();

			// Generate 1st level generic record reader (rows)
			GenericRecord avroRow = new GenericData.Record(avroSchema);

			// For all rows in file repeat
			while (dataFileReader.hasNext()) {
				// read the row
				avroRow = dataFileReader.next(avroRow);
				HashMap<String, String> tagMap = new HashMap<String, String>();

				// Generate 2nd level generic record reader (tags)
				HashMap tags = (HashMap) avroRow.get("tags");
				if (tags != null) {
					for (Object item : tags.keySet()) {
						tagMap.put(item.toString(), tags.get(item).toString());
					}
				}

				// Grab 1st level mandatory fields
				String type = avroRow.get("type").toString();
				String group = avroRow.get("group").toString();
				String subgroup = avroRow.get("subgroup").toString();

				// Insert data to list
				this.insert(type, group, subgroup, tagMap);

			} // end of avro rows

			this.unfilter();

			dataFileReader.close();

		} catch (IOException ex) {
			log.error("Could not open avro file:" + avroFile.getName());
			throw ex;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(dataFileReader);
		}

	}

}
