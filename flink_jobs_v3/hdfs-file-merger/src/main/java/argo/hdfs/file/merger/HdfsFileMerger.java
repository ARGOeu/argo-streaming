package argo.hdfs.file.merger;


import argo.avro.MetricData;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*** HdfsFileMerger merges in one AVRO file the existing files in the hdfs path
 and removes the multiple files
 * 
 *  * Submit job in flink cluster using the following parameters: 
 *--path(Required): the path to the hdfs folder . Based on this path, the job accesses the files for the date e.g --path hdfs://host:port/user/tenant/tenantA/mdata/YYYY-MM-DD 
 * 
 **/

public class HdfsFileMerger {

    static Logger LOG = LoggerFactory.getLogger(HdfsFileMerger.class);

    
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        String path=params.getRequired("path");
        Path pin=new Path(path);
        String mpath=path+"/mdata-0-0";
        AvroInputFormat<MetricData> pdataAvro=new AvroInputFormat<>(pin,MetricData.class);
        DataSet<MetricData> dataDS = env.createInput(pdataAvro);
    
        String fspath=splitPath(path); //filesystem path
       
        FileSystem fs = FileSystem.get(URI.create(fspath));
        FileStatus[] filestatus = fs.listStatus(pin); //retrieves the multiple files existing in the path and need to be merged
           if (filestatus.length <2) {
            LOG.info("mdata size< 2 : No merging required");
            return;
        }
         dataDS.write(new AvroOutputFormat<>(MetricData.class), mpath,FileSystem.WriteMode.OVERWRITE);
         env.execute();
       
          for (FileStatus fst : filestatus) {
            Path p = fst.getPath();
            if (!fst.isDir()) {
                fs.delete(p, true);
            }
        }
    }

  //splits the path and returns the filesystem path hdfs://host:port,  e.g hdfs://localhost:9000  
   private static String splitPath(String path){
    
   String[] parts = path.split(":");
   String port= parts[2].substring(0,4);
   return parts[0]+":"+parts[1]+":"+port;
    }

}
