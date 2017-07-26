package sync;

import org.apache.log4j.Logger;

import argo.avro.MetricProfile;
import argo.avro.Weight;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;

/**
 * WeightManager manages supplementary weight information that is needed in computation of a/r scores for endpoint groups
 * Information can be loaded either directly from an avro file or from a list of avro objects
 */
public class WeightManager {


	/**
	 * Hashmap that holds information to weight lists by type. 
	 */
	private HashMap<String, ArrayList<WeightItem>> list;
	
	private static final Logger LOG = Logger.getLogger(WeightManager.class.getName());

	/**
	 * Inner class that holds information about a weight item which is a actually a tuple (group_name,weight_value)
	 */
	private class WeightItem {
		String group; // name of the group
		String weight; // weight value

		public WeightItem() {
			// Initializations
			this.group = "";
			this.weight = "";

		}

		public WeightItem(String group, String weight) {
			this.group = group;
			this.weight = weight;
		}
	}

	public WeightManager() {
		list = new HashMap<String, ArrayList<WeightItem>>();
	}

	/**
	 * Inserts new weight information (type,group_name,weight_value) to the Weight manager
	 */
	public int insert(String type, String group, String weight) {
		WeightItem tmpItem = new WeightItem(group, weight);
		if (this.list.containsKey(type)) {
			this.list.get(type).add(tmpItem);
		} else {
			this.list.put(type, new ArrayList<WeightItem>());
			this.list.get(type).add(tmpItem);
		}

		return 0; // All good
	}

	/**
	 * Returns weight information by (type,group_name)
	 */
	public int getWeight(String type, String group) {
		if (list.containsKey(type)) {
			for (WeightItem item : list.get(type)) {
				if (item.group.equals(group)) {
					return Integer.parseInt(item.weight);
				}
			}
		}

		return 0;

	}

	/**
	 * Loads weight information from an avro file
	 * <p>
	 * This method loads weight information contained in an .avro file with
	 * specific avro schema.
	 * 
	 * <p>
	 * The following fields are expected to be found in each avro row:
	 * <ol>
	 * <li>type: string</li>
	 * <li>site: string</li>
	 * <li>weight: string</li>
	 * <li>[optional] tags: hashmap (contains a map of arbitrary key values)
	 * </li>
	 * </ol>
	 * 
	 * @param avroFile
	 *            a File object of the avro file that will be opened
	 * @throws IOException
	 *             if there is an error during opening of the avro file
	 */
	@SuppressWarnings("unchecked")
	public int loadAvro(File avroFile) throws IOException {

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

				HashMap<Utf8, String> tags = (HashMap<Utf8, String>) (avroRow.get("tags"));

				if (tags != null) {
					for (Utf8 item : tags.keySet()) {
						tagMap.put(item.toString(), String.valueOf(tags.get(item)));
					}
				}

				// Grab 1st level mandatory fields
				String type = avroRow.get("type").toString();
				String group = avroRow.get("site").toString();
				String weight = avroRow.get("weight").toString();

				// Insert data to list
				this.insert(type, group, weight);

			} // end of avro rows

			dataFileReader.close();

		} catch (IOException ex) {
			LOG.error("Could not open avro file:" + avroFile.getName());
			throw ex;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(dataFileReader);
		}

		return 0; // allgood
	}
	
	/**
	 * Loads Weight information from a list of Weight objects
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void loadFromList( List<Weight> wg)  {

		// For each weight in list
		for (Weight item : wg){
			String type = item.getType();
			String group = item.getSite();
			String weight = item.getWeight();
			// Insert data to list
			this.insert(type, group, weight);
		}
		

	}

}
