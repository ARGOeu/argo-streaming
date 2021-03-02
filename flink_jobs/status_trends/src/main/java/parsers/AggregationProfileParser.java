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

    private static String id;
    private static String date;
    private static String name;
    private static String namespace;
    private static String endpointGroup;
    private static String metricOp;
    private static String profileOp;
    private static String[] metricProfile = new String[2];
    private static ArrayList<GroupOps> groups=new ArrayList<>();

    public static void loadAggrProfileInfo(String baseUri, String key, String proxy) throws IOException, ParseException {

        JSONObject jsonObject = RequestManager.getAggregationProfileRequest(baseUri, key, proxy);

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
                                JSONObject servObject=(JSONObject)serviceiterator.next();
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

    public static String getId() {
        return id;
    }

    public static void setId(String id) {
        AggregationProfileParser.id = id;
    }

    public static String getDate() {
        return date;
    }

    public static void setDate(String date) {
        AggregationProfileParser.date = date;
    }

    public static String getName() {
        return name;
    }

    public static void setName(String name) {
        AggregationProfileParser.name = name;
    }

    public static String getNamespace() {
        return namespace;
    }

    public static void setNamespace(String namespace) {
        AggregationProfileParser.namespace = namespace;
    }

    public static String getEndpointGroup() {
        return endpointGroup;
    }

    public static void setEndpointGroup(String endpointGroup) {
        AggregationProfileParser.endpointGroup = endpointGroup;
    }

    public static String getMetricOp() {
        return metricOp;
    }

    public static void setMetricOp(String metricOp) {
        AggregationProfileParser.metricOp = metricOp;
    }

    public static String getProfileOp() {
        return profileOp;
    }

    public static void setProfileOp(String profileOp) {
        AggregationProfileParser.profileOp = profileOp;
    }

    public static String[] getMetricProfile() {
        return metricProfile;
    }

    public static void setMetricProfile(String[] metricProfile) {
        AggregationProfileParser.metricProfile = metricProfile;
    }

    public static ArrayList<GroupOps> getGroups() {
        return groups;
    }

    public static void setGroups(ArrayList<GroupOps> groups) {
        AggregationProfileParser.groups = groups;
    }

    
}