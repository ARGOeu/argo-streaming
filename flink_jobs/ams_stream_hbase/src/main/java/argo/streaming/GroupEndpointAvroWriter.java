package argo.streaming;

import java.io.IOException;
import java.util.Map;


import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FSDataOutputStream;


/**
 * Implements a specific AvroWriter For GroupEndpoint Avro Objects
 */
public class GroupEndpointAvroWriter<GroupEndpoint> implements Writer<GroupEndpoint> {

	
	private static final long serialVersionUID = 1L;
	
	private transient FSDataOutputStream outputStream = null;
	private transient DataFileWriter<GroupEndpoint> outputWriter = null;
	private final Map<String, String> properties;
	
	public GroupEndpointAvroWriter(Map<String, String> properties) {
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
		      DatumWriter<GroupEndpoint> writer  = new SpecificDatumWriter<GroupEndpoint>(argo.avro.MetricData.getClassSchema());
		      outputWriter = new DataFileWriter<GroupEndpoint>(writer);
		      
		      outputWriter.create(argo.avro.MetricData.getClassSchema(),outStream);
		    }
		
	}
	
	/**
	 * Write the avro element to the output writer
	 */
	@Override
	public void write(GroupEndpoint item) throws IOException {
		if (outputStream == null) {
		      throw new IllegalStateException("AvroWriter has not been opened.");
		    }
		    outputWriter.append(item);
		
	}

}
