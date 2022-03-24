package argo.streaming;

import com.google.common.base.Ascii;
import java.awt.BorderLayout;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TimeZone;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Accepts a list of events and add, removes fields to create a message as defined
 */
public class TrimEvent implements FlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;
    final ParameterTool params;
    private String historyUrl;
    private String reportName;
    private String egroup;
    private String tenant;
    

    public TrimEvent(ParameterTool params,String tenant, String reportName, String egroup) {
        this.params = params;
        this.historyUrl = this.params.get("url.history.endpoint");
        this.reportName = reportName;
        this.egroup = egroup;
        this.tenant=tenant;

    }

    static Logger LOG = LoggerFactory.getLogger(TrimEvent.class);

    @Override
    public void flatMap(String in, Collector<String> out) throws Exception {

        JSONObject json = new JSONObject(in);

        String type = json.getString("type");
        ArrayList<String> removedFields = new ArrayList();
        removedFields.add("monitoring_host");
        String initialHistPath = "";
        String dateParams="";
        if (this.historyUrl != null) {
            initialHistPath ="https://"+ this.historyUrl +"/"+this.tenant+"/" + "report-status/" + this.reportName + "/" + egroup + "/"; //constructs the initial path to the url 
           dateParams=buildDateParams(json.getString("ts_processed"));
            
        }
        switch (type) {
            case "metric":
                removedFields.add("status_metric");
                json = trimEvent(json, removedFields);
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.getString("endpoint_group") + "/" + json.getString("service") + "/" + json.getString("hostname")  + dateParams;
                    json.put("url.history", finalHistUrlPath);
                }
                break;
            case "endpoint":
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                removedFields.add("metric_statuses");
                removedFields.add("metric_names");
                json = trimEvent(json, removedFields);
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.getString("endpoint_group") + "/" + json.getString("service") + "/" + json.getString("hostname")+ dateParams;
                    json.put("url.history", finalHistUrlPath);
                }

                break;
            case "service":
                removedFields.add("status_service");
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                json = trimEvent(json, removedFields);
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.getString("endpoint_group") + "/" + json.getString("service") + dateParams;
                    json.put("url.history", finalHistUrlPath);
                }

                break;
            case "endpoint_group":
                removedFields.add("status_egroup");
                removedFields.add("status_service");
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                removedFields.add("group_statuses");
                removedFields.add("group_endpoints");
                removedFields.add("group_services");
                json = trimEvent(json, removedFields);
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.getString("endpoint_group") +dateParams;
                    json.put("url.history", finalHistUrlPath);
                }

                break;

            default:
                
        }
        out.collect(json.toString());

    }

    private JSONObject trimEvent(JSONObject event, List<String> fields) throws JSONException {

        for (String field : fields) {
            event.remove(field);
        }
        return event;
    }
    private  String buildDateParams(String dateStr) throws ParseException {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        DateTime dt = formatter.parseDateTime(dateStr);
        
              //String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
        String endDate = dt.toString(dtf);
  
        
        DateTime prevTime=dt.minusDays(2);
        String startDate = prevTime.toString(dtf);
  
        String param="?start_date="+startDate+"&end_date="+endDate;
        
        return param;
    }


}
