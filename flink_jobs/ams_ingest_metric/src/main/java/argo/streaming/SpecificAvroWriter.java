package argo.streaming;

import java.io.IOException;


import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Implements a specific AvroWriter For MetricData Avro Objects
 */
public class SpecificAvroWriter<T> extends StreamWriterBase<T> {

	
	private static final long serialVersionUID = 1L;
	
	private transient FSDataOutputStream outputStream = null;
	private transient DataFileWriter<T> outputWriter = null;
	
	
	public SpecificAvroWriter(){
		
	}
	
	
	
	
	
	@Override
	public void close() throws IOException {
		if(outputWriter != null) {
		      outputWriter.sync();
		      outputWriter.close();
		    }
		    outputWriter = null;
		    outputStream = null;
		
	}

	
	
	/**
	 * Write the avro element to the output writer
	 */
	@Override
	public void write(T item) throws IOException {
		if (outputStream == null) {
		      throw new IllegalStateException("AvroWriter has not been opened.");
		    }
		    outputWriter.append(item);
		   
		
	}

	@Override
	public void open(FileSystem fs, Path path) throws IOException {
		super.open(fs, path);
		 if (outputStream != null) {
		      throw new IllegalStateException("AvroWriter has already been opened.");
		    }
		    outputStream = getStream();

		    if(outputWriter == null) {
		      DatumWriter<T> writer  = new SpecificDatumWriter<T>(argo.avro.MetricData.getClassSchema());
		      outputWriter = new DataFileWriter<T>(writer);
		      
		      outputWriter.create(argo.avro.MetricData.getClassSchema(),outputStream);
		    }
		
	}

	@Override
	public long flush() throws IOException {
		if (outputWriter != null) {
			outputWriter.sync();
		}
		return super.flush();
	}

	@Override
	public Writer<T> duplicate() {
		// TODO Auto-generated method stub
		return new SpecificAvroWriter<T>();
	}

	
}
