package argo.commons.profiles;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import argo.commons.requests.RequestManager;

/**
 *
 * @author cthermolia AggregationProfileParser, collects data as described in
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

    public void readApiRequestResult() {
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

            JSONObject groupObject = (JSONObject) groupiterator.next();
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
                serviceFunctionList.add(groupname);
                serviceFunctions.put(servicename, serviceFunctionList);
            }
            groups.add(new GroupOps(groupname, groupoperation, services));

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

    public class GroupOps implements Serializable {

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

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 13 * hash + Objects.hashCode(this.name);
            hash = 13 * hash + Objects.hashCode(this.operation);
            hash = 13 * hash + Objects.hashCode(this.services);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final GroupOps other = (GroupOps) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (!Objects.equals(this.operation, other.operation)) {
                return false;
            }
            if (!Objects.equals(this.services, other.services)) {
                return false;
            }
            return true;
        }

    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.id);
        hash = 59 * hash + Objects.hashCode(this.date);
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.namespace);
        hash = 59 * hash + Objects.hashCode(this.endpointGroup);
        hash = 59 * hash + Objects.hashCode(this.metricOp);
        hash = 59 * hash + Objects.hashCode(this.profileOp);
        hash = 59 * hash + Arrays.deepHashCode(this.metricProfile);
        hash = 59 * hash + Objects.hashCode(this.groups);
        hash = 59 * hash + Objects.hashCode(this.serviceOperations);
        hash = 59 * hash + Objects.hashCode(this.functionOperations);
        hash = 59 * hash + Objects.hashCode(this.serviceFunctions);
        hash = 59 * hash + Objects.hashCode(this.url);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AggregationProfileParser other = (AggregationProfileParser) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.date, other.date)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.namespace, other.namespace)) {
            return false;
        }
        if (!Objects.equals(this.endpointGroup, other.endpointGroup)) {
            return false;
        }
        if (!Objects.equals(this.metricOp, other.metricOp)) {
            return false;
        }
        if (!Objects.equals(this.profileOp, other.profileOp)) {
            return false;
        }
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Arrays.deepEquals(this.metricProfile, other.metricProfile)) {
            return false;
        }
        if (!Objects.equals(this.groups, other.groups)) {
            return false;
        }
        if (!Objects.equals(this.serviceOperations, other.serviceOperations)) {
            return false;
        }
        if (!Objects.equals(this.functionOperations, other.functionOperations)) {
            return false;
        }
        if (!Objects.equals(this.serviceFunctions, other.serviceFunctions)) {
            return false;
        }
        return true;
    }

}