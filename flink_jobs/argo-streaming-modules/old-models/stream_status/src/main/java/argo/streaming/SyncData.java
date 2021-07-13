package argo.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import argo.avro.Weight;

/**
 * Utility class that can target text or connector avro encoded files (metric data,
 * metric profiles, topology, weights, downtimes etc). 
 */
public class SyncData {

	/**
	 * Convert a string url (mostly hdfs://) to URI object
	 */
	private URI toURI(String url) throws URISyntaxException {

		return new URI(url);

	}

	/**
	 * Utility class that can target text or connector avro encoded files (metric data,
	 * metric profiles, topology, weights, downtimes etc). 
	 */
	public String readText(String url) throws URISyntaxException, IOException {

		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedReader bfr = bfrHDFS(uri);
			return readTextFile(bfr);

		}

		return "";

	}

	/**
	 * Read a plain text file
	 */
	public String readTextFile(BufferedReader bfr) throws IOException {
		String line = null;
		String full = "";
		while ((line = bfr.readLine()) != null) {
			full = full + line;
		}

		return full;
	}

	/**
	 * Read a list of GroupEndpoint Avro Objects from url
	 */
	public ArrayList<GroupEndpoint> readGroupEndpoint(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readGroupEndpointFile(bis);

		}

		return new ArrayList<GroupEndpoint>();
	}

	/**
	 * Read a list of GroupEndpoint Avro Objects from an InputStream
	 */
	public ArrayList<GroupEndpoint> readGroupEndpointFile(BufferedInputStream bis) throws IOException {
		DatumReader<GroupEndpoint> datumReader = new SpecificDatumReader<GroupEndpoint>(GroupEndpoint.getClassSchema(),GroupEndpoint.getClassSchema(),new SpecificData());
		DataFileStream<GroupEndpoint> dataFileStream = new DataFileStream<GroupEndpoint>(bis, datumReader);

		ArrayList<GroupEndpoint> list = new ArrayList<GroupEndpoint>();

		while (dataFileStream.hasNext()) {
			// read the row
			GroupEndpoint cur = dataFileStream.next();
			list.add(cur);
		}

		dataFileStream.close();

		return list;
	}

	/**
	 * Read a list of GroupGroup Avro Objects from a url
	 */
	public ArrayList<GroupGroup> readGroupGroup(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readGroupGroupFile(bis);

		}

		return new ArrayList<GroupGroup>();
	}

	/**
	 * Read a list of GroupGroup Avro Objects from an input stream
	 */
	public ArrayList<GroupGroup> readGroupGroupFile(BufferedInputStream bis) throws IOException {
		DatumReader<GroupGroup> datumReader = new SpecificDatumReader<GroupGroup>(GroupGroup.getClassSchema(),GroupGroup.getClassSchema(),new SpecificData());
		DataFileStream<GroupGroup> dataFileStream = new DataFileStream<GroupGroup>(bis, datumReader);

		ArrayList<GroupGroup> list = new ArrayList<GroupGroup>();

		while (dataFileStream.hasNext()) {
			// read the row
			GroupGroup cur = dataFileStream.next();
			list.add(cur);
		}

		dataFileStream.close();

		return list;
	}

	/**
	 * Read a list of Downtime Avro Objects from a url
	 */
	public ArrayList<Downtime> readDowntime(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readDowntimeFile(bis);

		}

		return new ArrayList<Downtime>();
	}

	/**
	 * Read a list of Downtime Avro Objects from an input stream
	 */
	public ArrayList<Downtime> readDowntimeFile(BufferedInputStream bis) throws IOException {
		DatumReader<Downtime> datumReader = new SpecificDatumReader<Downtime>(Downtime.getClassSchema(),Downtime.getClassSchema(),new SpecificData());
		DataFileStream<Downtime> dataFileStream = new DataFileStream<Downtime>(bis, datumReader);

		ArrayList<Downtime> list = new ArrayList<Downtime>();

		while (dataFileStream.hasNext()) {
			// read the row
			Downtime cur = dataFileStream.next();
			list.add(cur);
		}

		dataFileStream.close();

		return list;
	}

	/**
	 * Read a list of MetricProfile Avro Objects from a url
	 */
	public ArrayList<MetricProfile> readMetricProfile(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readMetricProfileFile(bis);

		}

		return new ArrayList<MetricProfile>();
	}

	/**
	 * Read a list of MetricProfile Avro Objects from an input stream
	 */
	public ArrayList<MetricProfile> readMetricProfileFile(BufferedInputStream bis) throws IOException {
		DatumReader<MetricProfile> datumReader = new SpecificDatumReader<MetricProfile>(MetricProfile.getClassSchema(),MetricProfile.getClassSchema(),new SpecificData());
		DataFileStream<MetricProfile> dataFileStream = new DataFileStream<MetricProfile>(bis, datumReader);

		

		ArrayList<MetricProfile> list = new ArrayList<MetricProfile>();

		while (dataFileStream.hasNext()) {
			// read the row
			MetricProfile cur = dataFileStream.next();
			list.add(cur);
		}

		dataFileStream.close();

		return list;
	}

	/**
	 * Read a list of MetricData Avro Objects from a url
	 */
	public ArrayList<MetricData> readMetricData(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readMetricDataFile(bis);

		}

		return new ArrayList<MetricData>();
	}

	/**
	 * Read a list of MetricData Avro Objects from an input stream
	 */
	public ArrayList<MetricData> readMetricDataFile(BufferedInputStream bis) throws IOException {
		DatumReader<MetricData> datumReader = new SpecificDatumReader<MetricData>(MetricData.getClassSchema(),MetricData.getClassSchema(),new SpecificData());
		DataFileStream<MetricData> dataFileStream = new DataFileStream<MetricData>(bis, datumReader);

		ArrayList<MetricData> list = new ArrayList<MetricData>();

		while (dataFileStream.hasNext()) {
			// read the row
			MetricData cur = dataFileStream.next();
			list.add(cur);
		}

		dataFileStream.close();

		return list;
	}

	/**
	 * Read a list of Weight Avro Objects from a url
	 */
	public ArrayList<Weight> readWeight(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readWeightFile(bis);

		}

		return new ArrayList<Weight>();
	}

	/**
	 * Read a list of Weight Avro Objects from an input stream
	 */
	public ArrayList<Weight> readWeightFile(BufferedInputStream bis) throws IOException {
		DatumReader<Weight> datumReader = new SpecificDatumReader<Weight>(Weight.getClassSchema(),Weight.getClassSchema(),new SpecificData());
		DataFileStream<Weight> dataFileStream = new DataFileStream<Weight>(bis, datumReader);

		ArrayList<Weight> list = new ArrayList<Weight>();

		while (dataFileStream.hasNext()) {
			// read the row
			Weight cur = dataFileStream.next();
			list.add(cur);
		}

		dataFileStream.close();

		return list;
	}

	/**
	 * Read a list of GenericRecord Avro Objects from a url
	 */
	public String readGenericAvro(String url) throws URISyntaxException, IOException {
		URI uri;

		uri = toURI(url);

		String proto = uri.getScheme();
		if (proto.equalsIgnoreCase("hdfs")) {
			BufferedInputStream bis = bisHDFS(uri);
			return readGenericAvroFile(bis);

		}

		return "";
	}

	/**
	 * Read a list of GenericRecord Avro Objects from an input stream
	 */
	public String readGenericAvroFile(BufferedInputStream bis) throws IOException {

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(bis, datumReader);
		Schema avroSchema = dataFileStream.getSchema();

		GenericRecord avroRow = new GenericData.Record(avroSchema);

		String str = "";

		while (dataFileStream.hasNext()) {
			// read the row
			avroRow = dataFileStream.next(avroRow);
			str = str + avroRow.toString();
		}

		dataFileStream.close();

		return str;
	}

	/**
	 *  Create a buffered reader from hdfs uri 
	 */
	public BufferedReader bfrHDFS(URI uri) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort());
		FileSystem fs;

		fs = FileSystem.get(conf);
		BufferedReader bfr = new BufferedReader(new InputStreamReader(fs.open(new Path(uri.getPath()))));
		return bfr;

	}

	/**
	 *  Create a buffered input stream from hdfs uri
	 */
	public BufferedInputStream bisHDFS(URI uri) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort());
		FileSystem fs;

		fs = FileSystem.get(conf);
		BufferedInputStream bis = new BufferedInputStream(fs.open(new Path(uri.getPath())));
		return bis;

	}

}
