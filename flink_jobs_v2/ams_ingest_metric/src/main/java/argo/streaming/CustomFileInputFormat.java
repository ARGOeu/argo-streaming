package argo.streaming;


import argo.avro.MetricData;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.apache.avro.io.DatumReader;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.connector.file.sink.compactor.InputFormatBasedReader;
import org.apache.flink.core.fs.Path;


public class CustomFileInputFormat extends OutputStreamWriter {

    private  DatumReader<MetricData> avroReader;
    private MetricData readRecord;

    public CustomFileInputFormat(OutputStream out) {
        super(out);
    }

    @Override
    public void close() throws IOException {
        super.close(); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    @Override
    public void flush() throws IOException {
        super.flush(); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    @Override
    public void write(int c) throws IOException {
        super.write(c); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    
    
    
    
    
}