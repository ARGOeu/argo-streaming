package argo.streaming;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricProfile;
import argo.avro.Weight;

/**
 * Custom Output format for storing Sync Data to HDFS
 */
class SyncHDFSOutputFormat implements OutputFormat<String> {

	// setup logger
	static Logger LOG = LoggerFactory.getLogger(SyncHDFSOutputFormat.class);

	private static final long serialVersionUID = 1L;

	private URI basePath;
	private org.apache.hadoop.conf.Configuration hadoopConf;
	private FileSystem hdfs;

	public void setBasePath(String url) throws URISyntaxException {
		this.basePath = new URI(url);
	}

	@Override
	public void configure(Configuration parameters) {

	}

	/**
	 * Initialize hadoop configuration and hdfs object
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		// create hdfs configuration
		hadoopConf = new org.apache.hadoop.conf.Configuration();
		hadoopConf.set("fs.defaultFS", basePath.getScheme() + "://" + basePath.getHost() + ":" + basePath.getPort());
		hdfs = FileSystem.get(hadoopConf);

	}

	/**
	 * Accepts a binary payload of avro group endpoint records and creates an avro file in designated hdfs path
	 */
	private void writeGroupEndpoint(byte[] payload, Path file) throws IllegalArgumentException, IOException {
		if (hdfs == null) {
			return;
		}
		FSDataOutputStream os = hdfs.create(file);
		DatumReader<GroupEndpoint> avroReader = new SpecificDatumReader<GroupEndpoint>(GroupEndpoint.getClassSchema(),
				GroupEndpoint.getClassSchema(), new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);

		DatumWriter<GroupEndpoint> avroWriter = new SpecificDatumWriter<GroupEndpoint>(GroupEndpoint.getClassSchema(),
				new SpecificData());
		DataFileWriter<GroupEndpoint> dfw = new DataFileWriter<GroupEndpoint>(avroWriter);
		dfw.create(GroupEndpoint.getClassSchema(), os);

		while (!decoder.isEnd()) {
			GroupEndpoint cur = avroReader.read(null, decoder);
			dfw.append(cur);

		}

		dfw.close();
		os.close();
	}

	/**
	 * Accepts a binary payload of avro group of groups records and creates an avro file in designated hdfs path
	 */
	private void writeGroupGroup(byte[] payload, Path file) throws IllegalArgumentException, IOException {
		if (hdfs == null) {
			return;
		}
		FSDataOutputStream os = hdfs.create(file);
		DatumReader<GroupGroup> avroReader = new SpecificDatumReader<GroupGroup>(GroupGroup.getClassSchema(),
				GroupGroup.getClassSchema(), new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);

		DatumWriter<GroupGroup> avroWriter = new SpecificDatumWriter<GroupGroup>(GroupGroup.getClassSchema(),
				new SpecificData());
		DataFileWriter<GroupGroup> dfw = new DataFileWriter<GroupGroup>(avroWriter);
		dfw.create(GroupGroup.getClassSchema(), os);

		while (!decoder.isEnd()) {
			GroupGroup cur = avroReader.read(null, decoder);
			dfw.append(cur);

		}

		dfw.close();
		os.close();
	}

	/**
	 * Accepts a binary payload of weight records and creates an avro file in designated hdfs path
	 */
	private void writeWeight(byte[] payload, Path file) throws IllegalArgumentException, IOException {
		if (hdfs == null) {
			return;
		}
		FSDataOutputStream os = hdfs.create(file);
		DatumReader<Weight> avroReader = new SpecificDatumReader<Weight>(Weight.getClassSchema(),
				Weight.getClassSchema(), new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);

		DatumWriter<Weight> avroWriter = new SpecificDatumWriter<Weight>(Weight.getClassSchema(), new SpecificData());
		DataFileWriter<Weight> dfw = new DataFileWriter<Weight>(avroWriter);
		dfw.create(Weight.getClassSchema(), os);

		while (!decoder.isEnd()) {
			Weight cur = avroReader.read(null, decoder);
			dfw.append(cur);

		}

		dfw.close();
		os.close();
	}

	/**
	 * Accepts a binary payload of avro downtime records and creates an avro file in designated hdfs path
	 */
	private void writeDowntime(byte[] payload, Path file) throws IllegalArgumentException, IOException {
		if (hdfs == null) {
			return;
		}
		FSDataOutputStream os = hdfs.create(file);
		DatumReader<Downtime> avroReader = new SpecificDatumReader<Downtime>(Downtime.getClassSchema(),
				Downtime.getClassSchema(), new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);

		DatumWriter<Downtime> avroWriter = new SpecificDatumWriter<Downtime>(Downtime.getClassSchema(),
				new SpecificData());
		DataFileWriter<Downtime> dfw = new DataFileWriter<Downtime>(avroWriter);
		dfw.create(Downtime.getClassSchema(), os);

		while (!decoder.isEnd()) {
			Downtime cur = avroReader.read(null, decoder);
			dfw.append(cur);

		}

		dfw.close();
		os.close();
	}

	/**
	 * Accepts a binary payload of metric profile records and creates an avro file in designated hdfs path
	 */
	private void writeMetricProfile(byte[] payload, Path file) throws IllegalArgumentException, IOException {
		if (hdfs == null) {
			return;
		}
		FSDataOutputStream os = hdfs.create(file);
		DatumReader<MetricProfile> avroReader = new SpecificDatumReader<MetricProfile>(MetricProfile.getClassSchema(),
				MetricProfile.getClassSchema(), new SpecificData());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);

		DatumWriter<MetricProfile> avroWriter = new SpecificDatumWriter<MetricProfile>(MetricProfile.getClassSchema(),
				new SpecificData());
		DataFileWriter<MetricProfile> dfw = new DataFileWriter<MetricProfile>(avroWriter);
		dfw.create(MetricProfile.getClassSchema(), os);

		while (!decoder.isEnd()) {
			MetricProfile cur = avroReader.read(null, decoder);
			dfw.append(cur);

		}

		dfw.close();
		os.close();
	}

	/**
	 * Accepts an AMS json message, parses it's attributes and decodes the data payload. 
	 * Then according to the attributes select an appropriate sync writing method to
	 * store the data as an hdfs avro file
	 */
	@Override
	public void writeRecord(String record) throws IOException {
		if (hdfs == null) {
			return;
		}

		JsonParser jsonParser = new JsonParser();
		// parse the json root object
		JsonElement jRoot = jsonParser.parse(record);
		// parse the json field "data" and read it as string
		// this is the base64 string payload
		String data = jRoot.getAsJsonObject().get("data").getAsString();
		// Decode from base64
		byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
		JsonElement jAttr = jRoot.getAsJsonObject().get("attributes");
		Map<String, String> attr = SyncParse.parseAttributes(jAttr);

		if (attr.containsKey("type") && attr.containsKey("report") && attr.containsKey("partition_date")) {

			String sType = attr.get("type");
			String sReport = attr.get("report");
			String sPdate = attr.get("partition_date");

			Path path = new Path(basePath.toString() + "/" + sReport + "/" + sType + "_" + sPdate + ".avro");
			LOG.info("Saving to:" + path.toString());

			if (sType.equalsIgnoreCase("metric_profile")) {
				writeMetricProfile(decoded64, path);
			} else if (sType.equalsIgnoreCase("group_endpoints")) {
				writeGroupEndpoint(decoded64, path);
			} else if (sType.equalsIgnoreCase("group_groups")) {
				writeGroupGroup(decoded64, path);
			} else if (sType.equalsIgnoreCase("downtimes")) {
				writeDowntime(decoded64, path);
			} else if (sType.equalsIgnoreCase("weights")) {
				writeWeight(decoded64, path);
			}
		}

	}

	@Override
	public void close() throws IOException {
		if (hdfs != null) {
			hdfs.close();
		}
	}

}