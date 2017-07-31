package argo.streaming;

import java.io.IOException;
import java.util.Map;


import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FSDataOutputStream;


/**
 * Implements a specific AvroWriter For MetricProfile Avro Objects
 */
public class MetricProfileAvroWriter<MetricProfile> implements Writer<MetricProfile> {

	
	private static final long serialVersionUID = 1L;
	
	private transient FSDataOutputStream outputStream = null;
	private transient DataFileWriter<MetricProfile> outputWriter = null;
	private final Map<String, String> properties;
	
	public MetricProfileAvroWriter(Map<String, String> properties) {
		this.properties = properties;
	}
	
	@Override
	public void close() throws IOException {
		if(outputWriter != null) {
		      outputWriter.sync();
		    }
		    outputWriter = null;
		    outputStream = null;
		
	}

	@Override
	public Writer duplicate() {
		
		return null;
	}

	@Override
	public void flush() throws IOException {
		
		
	}
	
	/**
	 * Establish the output stream and output writer
	 */
	@Override
	public void open(FSDataOutputStream outStream) throws IOException {
		 if (outputStream != null) {
		      throw new IllegalStateException("AvroWriter has already been opened.");
		    }
		    outputStream = outStream;

		    if(outputWriter == null) {
		      DatumWriter<MetricProfile> writer  = new SpecificDatumWriter<MetricProfile>(argo.avro.MetricProfile.getClassSchema());
		      outputWriter = new DataFileWriter<MetricProfile>(writer);
		      
		      outputWriter.create(argo.avro.MetricData.getClassSchema(),outStream);
		    }
		
	}
	
	/**
	 * Write the avro element to the output writer
	 */
	@Override
	public void write(MetricProfile item) throws IOException {
		if (outputStream == null) {
		      throw new IllegalStateException("AvroWriter has not been opened.");
		    }
		    outputWriter.append(item);
		
	}

}
