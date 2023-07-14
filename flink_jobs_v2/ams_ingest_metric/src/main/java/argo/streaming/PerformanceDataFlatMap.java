package argo.streaming;

import argo.avro.MetricData;
import com.influxdb.client.domain.WritePrecision;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import com.influxdb.client.write.Point;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PerformanceDataFlatMap implements a map function that creates InfluxDB
 * Points, that keeps the information for performance, generated from the actual
 * data MetricData includes
 */
public class PerformanceDataFlatMap extends RichFlatMapFunction< Tuple2<String, MetricData>, Point> {

    private static final long serialVersionUID = 1L;
    static Logger LOG = LoggerFactory.getLogger(PerformanceDataFlatMap.class);

    public PerformanceDataFlatMap() {
    }

    /**
     * Initializes constructs in the beginning of operation
     *
     * @param parameters Configuration parameters to initialize structures
     * @throws URISyntaxException
     */
    @Override
    public void open(Configuration parameters) throws IOException, ParseException, URISyntaxException {

    }

    //FlatMap to retrieve actual data from MetricData , parse them and create the Point expressing performance data , to be written in influxdb
      @Override
    public void flatMap(Tuple2<String, MetricData> in, Collector<Point> out) throws Exception {

        ArrayList<Tuple8> tuples = parsePerformanceData(in.f0, in.f1); //parsing MetricData to retrieve actual data and the info representing performance
        for (Tuple8<String, String, String, String, String, String, String, String> tuple : tuples) {

            HashMap<String, String> tags = new HashMap<>();
            tags.put("group", tuple.f2);
            tags.put("service", tuple.f3);
            tags.put("endpoint", tuple.f4);
            tags.put("metric", tuple.f5);

            HashMap<String, Object> fields = new HashMap<>();
            fields.put("value", tuple.f6);
            fields.put("unit", tuple.f7);
            String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

            SimpleDateFormat sdf = new SimpleDateFormat(format);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date = sdf.parse(tuple.f1);
         
            Point p = Point.measurement(tuple.f0).time(date.getTime(), WritePrecision.MS).addTags(tags).addFields(fields);
            out.collect(p);
        }
    }

    
    private static ArrayList<Tuple8> parsePerformanceData(String group, MetricData item) {

        ArrayList<Tuple8> list = new ArrayList<>();
        String timestamp = item.getTimestamp();
        String service = item.getService();
        String hostname = item.getHostname();
        String metric = item.getMetric();

        String actualData = item.getActualData();
        if(actualData==null){
        return new ArrayList<>();
        }
        ArrayList<PerformanceValue> performanceValues = splitActualData(actualData);//splits the actual data string to retrieve the info about performace data

        for (PerformanceValue val : performanceValues) {

            Tuple8<String, String, String, String, String, String, Float, String> perfData = new Tuple8<String, String, String, String, String, String, Float, String>(
                    val.label, timestamp, group, service, hostname, metric, val.value, val.unit);
            list.add(perfData);
        }
        return list;
    }

    private static ArrayList<PerformanceValue> splitActualData(String initialString) {

        //example :"time=2.227776s;;;0.000000 size=69485B;;;0"

        String[] actualDataSplit = initialString.split(" "); //splits on " " to retrieve the various performance entities (e.g time,size)
        ArrayList<PerformanceValue> perfValueList = new ArrayList<>();
        for (String actualData : actualDataSplit) { //for each performance entity e.g time=2.227776s;;;0.000000
            String[] labelSplit = actualData.split("="); //split on "=" to seperate the name and the values 
            String label = labelSplit[0]; // the first part is the name of the entity e.g time
            if(label.equals("time")){
             String[] values = labelSplit[1].split(";"); //the second part contains the value-unit and the min,max limits e.g 2.227776s;;;0.000000. we split on ; to get the value-unit
            String value = values[0]; //gets the value-unit e.g  2.227776s
             
            String unit = value.replaceAll("[^A-Za-z]", ""); //we recognize the unit by the alpharithmetic part e.g s
            String numericVal = value.replaceAll("[^0-9]", ""); //we recognize the value by the numeric part e.g 2.227776
            Float floatVal=Float.valueOf(numericVal);
            PerformanceValue perfValue = new PerformanceValue(label, floatVal, unit); // we keep the performance entity as name,value,unit e.g [time,2.227776,s]
            perfValueList.add(perfValue);
            }
        }
        return perfValueList;

    }

  
    private static class PerformanceValue {

        private String label;
        private Float value;
        private String unit;

        public PerformanceValue(String label, Float value, String unit) {
            this.label = label;
            this.value = value;
            this.unit = unit;
        }

    }

}
