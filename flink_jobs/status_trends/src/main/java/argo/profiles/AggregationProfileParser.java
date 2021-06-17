/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;

import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 * AggregationProfileParser, collects data as described in
 * the json received from web api aggregation profiles request
 */

public class AggregationProfileParser implements Serializable {
    private String id;
    private String date;
    private String name;
    private String namespace;
    private String endpointGroup;
    private String metricOp;
    private String profileOp;
    private String[] metricProfile = new String[2];
    private ArrayList<GroupOps> groups = new ArrayList<>();

    private HashMap<String, String> serviceOperations = new HashMap<>();
    private HashMap<String, String> functionOperations = new HashMap<>();
    private HashMap<String, ArrayList<String>> serviceFunctions = new HashMap<>();
    private final String url = "/aggregation_profiles";
    private JSONObject jsonObject;
    public AggregationProfileParser() {
    }

    
    public AggregationProfileParser(String apiUri, String key, String proxy, String aggregationId, String dateStr) throws IOException, ParseException {

        String uri = apiUri + url + "/" + aggregationId;
        if (dateStr != null) {
            uri = uri + "?date=" + dateStr;
        }

        loadAggrProfileInfo(uri, key, proxy);
    }

    public AggregationProfileParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }
    

    public void loadAggrProfileInfo(String uri, String key, String proxy) throws IOException, ParseException {
           jsonObject = RequestManager.request(uri, key, proxy);
           readApiRequestResult();
    }
       public void readApiRequestResult(){
        JSONArray dataList = (JSONArray) jsonObject.get("data");

       JSONObject dataObject = (JSONObject) dataList.get(0);

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
            JSONObject groupObject=groupiterator.next();
            if (groupObject instanceof JSONObject) {
              
                String groupname = (String) groupObject.get("name");
                String groupoperation = (String) groupObject.get("operation");

                functionOperations.put(groupname, groupoperation);
                JSONArray serviceArray = (JSONArray) groupObject.get("services");
                Iterator<JSONObject> serviceiterator = serviceArray.iterator();
                HashMap<String, String> services = new HashMap<>();
                 while (serviceiterator.hasNext()) {
                    JSONObject servObject = (JSONObject) serviceiterator.next();
                    String servicename = (String) servObject.get("name");
                    String serviceoperation = (String) servObject.get("operation");
                    serviceOperations.put(servicename, serviceoperation);
                    services.put(servicename, serviceoperation);

                    ArrayList<String> serviceFunctionList = new ArrayList<>();
                    if (serviceFunctions.get(servicename) != null) {
                        serviceFunctionList = serviceFunctions.get(servicename);
                   }
                    serviceFunctionList.add(serviceoperation);
                    serviceFunctions.put(servicename, serviceFunctionList);
                }
              groups.add(new GroupOps(groupname, groupoperation, services));

            }
        }
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
       

    public ArrayList<String> retrieveServiceFunctions(String service) {
        return serviceFunctions.get(service);

   }

    public String getServiceOperation(String service) {
        return serviceOperations.get(service);

    }

 public String getFunctionOperation(String function) {

        return functionOperations.get(function);

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

    public HashMap<String, String> getServiceOperations() {
        return serviceOperations;
    }

    public void setServiceOperations(HashMap<String, String> serviceOperations) {
        this.serviceOperations = serviceOperations;
    }
    public HashMap<String, String> getFunctionOperations() {
        return functionOperations;
    }

    public void setFunctionOperations(HashMap<String, String> functionOperations) {
        this.functionOperations = functionOperations;
    }

    public HashMap<String, ArrayList<String>> getServiceFunctions() {
        return serviceFunctions;
    }

    public void setServiceFunctions(HashMap<String, ArrayList<String>> serviceFunctions) {
        this.serviceFunctions = serviceFunctions;
    }
    public static class GroupOps implements Serializable {

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
