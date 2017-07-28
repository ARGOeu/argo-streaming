package sync;

import org.apache.log4j.Logger;

import argo.avro.Downtime;
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
 * DowntimeManager manages supplementary downtime information that is needed in computation of a/r scores for endpoint groups
 * Information can be loaded either directly from an avro file or from a list of avro objects
 */
public class DowntimeManager {

	/**
	 * List of Downtime information items
	 */
	private ArrayList<DowntimeItem> list;
	private static final Logger LOG = Logger.getLogger(DowntimeManager.class.getName());

	/**
	 * Inner class that holds information about a downtime item which is a actually a 4-tuple (hostname,service,startTime,endTime)
	 */
	private class DowntimeItem {
		String hostname; // name of host
		String service; // name of service
		String startTime; // declare start time of downtime
		String endTime; // declare end time of downtime

		public DowntimeItem() {
			// Initializations
			this.hostname = "";
			this.service = "";
			this.startTime = "";
			this.endTime = "";
		}

		public DowntimeItem(String hostname, String service, String startTime, String endTime) {
			this.hostname = hostname;
			this.service = service;
			this.startTime = startTime;
			this.endTime = endTime;
		}

	}

	public DowntimeManager() {
		this.list = new ArrayList<DowntimeItem>();
	}
	/**
	 * Inserts new downtime information to Donwtime Manager (hostname,service,startTime,endTime)
	 */
	public int insert(String hostname, String service, String startTime, String endTime) {
		DowntimeItem tmpItem = new DowntimeItem(hostname, service, startTime, endTime);
		this.list.add(tmpItem);
		return 0; // All good
	}

	/**
	 * Returns the downtime period (if any) for a specific service endpoint: (hostname,service)
	 */
	public ArrayList<String> getPeriod(String hostname, String service) {

		ArrayList<String> period = new ArrayList<String>();

		for (DowntimeItem item : this.list) {

			if (item.hostname.equals(hostname)) {
				if (item.service.equals(service)) {
					period.add(item.startTime);
					period.add(item.endTime);
					return period;
				}
			}
		}

		return null;

	}

	/**
	 * Loads downtime information from an avro file
	 * <p>
	 * This method loads downtimes information contained in an .avro file with
	 * specific avro schema.
	 * 
	 * <p>
	 * The following fields are expected to be found in each avro row:
	 * <ol>
	 * <li>start_time: string</li>
	 * <li>end_time: string</li>
	 * <li>service: string</li>
	 * <li>hostname: string</li>
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

				HashMap<Utf8, String> tags = (HashMap<Utf8, String>) (avroRow.get("tags"));

				if (tags != null) {
					for (Utf8 item : tags.keySet()) {
						tagMap.put(item.toString(), String.valueOf(tags.get(item)));
					}
				}

				// Grab 1st level mandatory fields
				String hostname = avroRow.get("hostname").toString();
				String service = avroRow.get("service").toString();
				String startTime = avroRow.get("start_time").toString();
				String endTime = avroRow.get("end_time").toString();

				// Insert data to list
				this.insert(hostname, service, startTime, endTime);

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
	 * Loads downtime information from a list of downtime objects
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void loadFromList( List<Downtime> dnt)  {
		// IF no downtimes collected return
		if (dnt==null) return;
		
		// For each downtime object in list
		for (Downtime item : dnt){
			String hostname = item.getHostname();
			String service = item.getService();
			String startTime = item.getStartTime();
			String endTime = item.getEndTime();
			// Insert data to list
			this.insert(hostname,service,startTime,endTime);
		}
		

	}

}
