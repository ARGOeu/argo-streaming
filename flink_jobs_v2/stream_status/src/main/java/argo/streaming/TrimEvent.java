package argo.streaming;

import com.google.common.base.Ascii;
import java.awt.BorderLayout;
import java.util.ArrayList;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Accepts a list of monitoring timelines and produces an endpoint timeline The
 * class is used as a RichGroupReduce Function in flink pipeline
 */
public class TrimEvent implements FlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    public TrimEvent() {
    }

    static Logger LOG = LoggerFactory.getLogger(TrimEvent.class);

    @Override
    public void flatMap(String in, Collector<String> out) throws Exception {

        JSONObject json = new JSONObject(in);

        String type = json.getString("type");
        ArrayList<String> removedFields = new ArrayList();
        removedFields.add("monitoring_host");
        switch (type) {
            case "metric":
                removedFields.add("status_metric");
               json=   trimEvent(json, removedFields);
              break;
            case "endpoint":
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                removedFields.add("metric_statuses");
                removedFields.add("metric_names");
              json=  trimEvent(json, removedFields);
                break;
            case "service":
                removedFields.add("status_service");
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                json=trimEvent(json, removedFields);

                break;
            case "endpoint_group":
                removedFields.add("status_egroup");
                removedFields.add("status_service");
                removedFields.add("status_endpoint");
                removedFields.add("status_metric");
                removedFields.add("group_statuses");
                removedFields.add("group_endpoints");
                removedFields.add("group_services");
                json=trimEvent(json, removedFields);

                break;

            default:
        }
        out.collect(json.toString());

    }

    public JSONObject trimEvent(JSONObject event, List<String> fields) throws JSONException {

        for (String field : fields) {
            event.remove(field);
        }
        return event;
    }

}
