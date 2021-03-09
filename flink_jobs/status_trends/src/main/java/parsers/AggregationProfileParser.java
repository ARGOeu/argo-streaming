/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parsers;

import argo.utils.RequestManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class AggregationProfileParser {

    private String id;
    private String date;
    private String name;
    private String namespace;
    private String endpointGroup;
    private String metricOp;
    private String profileOp;
    private String[] metricProfile = new String[2];
    private ArrayList<GroupOps> groups = new ArrayList<>();

    public AggregationProfileParser(String baseUri, String key, String proxy, String aggregationId, String dateStr) throws IOException, ParseException {

        loadAggrProfileInfo(baseUri, key, proxy, aggregationId, dateStr);
    }

    public void loadAggrProfileInfo(String baseUri, String key, String proxy, String aggregationId, String dateStr) throws IOException, ParseException {

        JSONObject jsonObject = RequestManager.getAggregationProfileRequest(baseUri, key, proxy, aggregationId, dateStr);

        JSONArray dataList = (JSONArray) jsonObject.get("data");

        Iterator<JSONObject> iterator = dataList.iterator();

        while (iterator.hasNext()) {
            if (iterator.next() instanceof JSONObject) {
                JSONObject dataObject = (JSONObject) iterator.next();

                id = (String) dataObject.get("id");
                date = (String) dataObject.get("date");
                name = (String) dataObject.get("name");
                namespace = (String) dataObject.get("namespace");
                endpointGroup = (String) dataObject.get("endpoint_group");
                metricOp = (String) dataObject.get("metric_operation");
                profileOp = (String) dataObject.get("profile_operation");

                JSONObject metricProfileObject = (JSONObject) dataObject.get("metric_profile");

                metricProfile[0] = (String) metricProfileObject.get("id");
                metricProfile[1] = (String) metricProfileObject.get("name");

                JSONArray groupArray = (JSONArray) dataObject.get("groups");
                Iterator<JSONObject> groupiterator = groupArray.iterator();

                while (groupiterator.hasNext()) {
                    if (groupiterator.next() instanceof JSONObject) {
                        JSONObject groupObject = (JSONObject) groupiterator.next();
                        String groupname = (String) groupObject.get("name");
                        String groupoperation = (String) groupObject.get("operation");

                        JSONArray serviceArray = (JSONArray) groupObject.get("services");
                        Iterator<JSONObject> serviceiterator = serviceArray.iterator();
                        HashMap<String, String> services = new HashMap<>();
                        while (serviceiterator.hasNext()) {
                            JSONObject servObject = (JSONObject) serviceiterator.next();
                            String servicename = (String) servObject.get("name");
                            String serviceoperation = (String) servObject.get("operation");
                            services.put(servicename, serviceoperation);

                        }
                        groups.add(new GroupOps(groupname, groupoperation, services));

                    }
                }
            }
        }
    }

    public String getId() {
        return id;
    }

    public String getDate() {
        return date;
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getEndpointGroup() {
        return endpointGroup;
    }

    public String getMetricOp() {
        return metricOp;
    }

    public String getProfileOp() {
        return profileOp;
    }

    public String[] getMetricProfile() {
        return metricProfile;
    }

    public ArrayList<GroupOps> getGroups() {
        return groups;
    }
    

    public static class GroupOps {

        private String name;
        private String operation;
        private HashMap<String, String> services;

        public GroupOps(String name, String operation, HashMap<String, String> services) {
            this.name = name;
            this.operation = operation;
            this.services = services;
        }

        public String getName() {
            return name;
        }

        public String getOperation() {
            return operation;
        }

        public HashMap<String, String> getServices() {
            return services;
        }

    }

}
