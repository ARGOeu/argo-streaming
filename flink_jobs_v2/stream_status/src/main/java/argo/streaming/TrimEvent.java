package argo.streaming;

import com.google.common.base.Ascii;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.awt.BorderLayout;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Accepts a list of events and add, removes fields to create a message as
 * defined
 */
public class TrimEvent implements FlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;
    final ParameterTool params;
    private String helpUrl;
    private String historyUrl;
    private String reportName;
    private String egroup;
    private String tenant;

    public TrimEvent(ParameterTool params, String tenant, String reportName, String egroup) {
        this.params = params;
        this.helpUrl = this.params.get("url.help");
        this.historyUrl = this.params.get("url.history.endpoint");
        this.reportName = reportName;
        this.egroup = egroup;
        this.tenant = tenant;

    }

    static Logger LOG = LoggerFactory.getLogger(TrimEvent.class);

    @Override
    public void flatMap(String in, Collector<String> out) throws Exception {

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(in);
        JsonObject json = jElement.getAsJsonObject();

        String type = json.get("type").getAsString();
        ArrayList<String> removedFields = new ArrayList();
        removedFields.add("monitoring_host");
        String initialHistPath = "";
        String dateParams = "";
        if (this.historyUrl != null) {
            initialHistPath = "https://" + this.historyUrl + "/" + this.tenant + "/" + "report-status/" + this.reportName + "/" + egroup + "/"; //constructs the initial path to the url 
            dateParams = buildDateParams(json.get("ts_processed").getAsString());

        }
        switch (type) {
            case "metric":
                removedFields.add("status_metric");
                json = trimEvent(json, removedFields);
                if (this.helpUrl != null) {
                    String helpUrlPath = "https://" + this.helpUrl + "/" + json.get("metric").getAsString(); //the help url is in the form of helpUrl/metric e.g https://poem.egi.eu/ui/public_metrics/metricA
                    json.addProperty("url.help", uriString(helpUrlPath));

                }
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.get("endpoint_group").getAsString() + "/" + json.get("service").getAsString() + "/" + json.get("hostname").getAsString() + dateParams;
                    json.addProperty("url.history", uriString(finalHistUrlPath));
                }
                break;
            case "endpoint":
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                removedFields.add("metric_statuses");
                removedFields.add("metric_names");
                json = trimEvent(json, removedFields);
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.get("endpoint_group").getAsString() + "/" + json.get("service").getAsString() + "/" + json.get("hostname").getAsString() + dateParams;
                    json.addProperty("url.history", uriString(finalHistUrlPath));
                }

                break;
            case "service":
                removedFields.add("status_service");
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                json = trimEvent(json, removedFields);
                if (this.historyUrl != null) {
                    String finalHistUrlPath = initialHistPath + json.get("endpoint_group").getAsString() + "/" + json.get("service").getAsString() + dateParams;
                    json.addProperty("url.history", uriString(finalHistUrlPath));
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
                    String finalHistUrlPath = initialHistPath + json.get("endpoint_group").getAsString() + dateParams;
                    json.addProperty("url.history", uriString(finalHistUrlPath));
                }

                break;

            default:

        }
        out.collect(json.toString());

    }

    private JsonObject trimEvent(JsonObject event, List<String> fields) {

        for (String field : fields) {
            event.remove(field);
        }
        return event;
    }

    private String buildDateParams(String dateStr) throws ParseException {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

        DateTime dt = formatter.parseDateTime(dateStr);

        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
        String endDate = dt.toString(dtf);

        DateTime prevTime = dt.minusDays(2);
        String startDate = prevTime.toString(dtf);

        String param = "?start_date=" + startDate + "&end_date=" + endDate;

        return param;
    }

    private String uriString(String urlpath) throws UnsupportedEncodingException, MalformedURLException, URISyntaxException {
        URI uri = new URI(urlpath);
        return uri.toString();

    }

}
