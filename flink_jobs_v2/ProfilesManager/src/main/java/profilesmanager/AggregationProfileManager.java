package profilesmanager;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.Serializable;
import java.util.Map.Entry;

/**
 *
 *
 * AggregationProfileManager class implements objects that store the information
 * parsed from a json object containing aggregation profile data or loaded from
 * an json file
 *
 * The AggregationProfileManager keeps info of a list of AvProfileItem objects
 * each one representing the aggregation profile information that corresponds to
 * a specific client's report
 */
public class AggregationProfileManager implements Serializable {

    private HashMap<String, AvProfileItem> list;
    private static final Logger LOG = Logger.getLogger(AggregationProfileManager.class.getName());
    private HashMap<String, String> profileNameIdMap;

    public AggregationProfileManager() {

        this.list = new HashMap<String, AvProfileItem>();
        this.profileNameIdMap = new HashMap<String, String>();

    }
  /**
     *
     * A AvProfileItem class implements an object containing info of an aggregation profile element
     */
    public class AvProfileItem implements Serializable {

        private String id;
        private String name;
        private String namespace;
        private String metricProfile;
        private String metricOp;
        private String groupType;
        private String op;

        private HashMap<String, ServGroupItem> groups; // a map of function(group) as key and a ServGroupItem as value. ServGroupItem keeps info of each service, operation pair in the profile data
        private HashMap<String, String> serviceIndex; // a map of service as key and an ArrayList of functions(groups) as value that correspond to the specific service
        private HashMap<String, String> serviceOperations; //a map of service as key and operation as value 
//        private HashMap<String, ArrayList<String>> serviceFunctions; 
        private HashMap<String, String> groupOperations; //a map of function(group) as key and operation as value, corresponding to the specific group

        AvProfileItem() {
            this.groups = new HashMap<String, ServGroupItem>();
            this.serviceIndex = new HashMap<String, String>();
            this.serviceOperations = new HashMap<String, String>();
            this.groupOperations = new HashMap<String, String>();
        }

        public class ServGroupItem implements Serializable {

            String op;
            HashMap<String, String> services;

            ServGroupItem(String op) {
                this.op = op;
                this.services = new HashMap<String, String>();
            }
        }

        /**
         * creates a ServGroupItem object and keeps in a map the pair of group,
         * ServGroupItem
         *
         * @param group, a function e.g "compute"
         * @param op , the operation that corresponds to the function e.g "AND"
         */
        public void insertGroup(String group, String op) {
            if (!this.groups.containsKey(group)) {
                this.groups.put(group, new ServGroupItem(op));
            }
        }

        /**
         * keeps in a map for each function the pair of service, operation
         *
         * @param group , a function e.g "compute"
         * @param service , a service e.g "ARC-CE"
         * @param op , an operation e.g "AND"
         */
        public void insertService(String group, String service, String op) {
            if (this.groups.containsKey(group)) {
                this.groups.get(group).services.put(service, op);

                this.serviceIndex.put(service, group);
            }
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public String getMetricProfile() {
            return metricProfile;
        }

        public void setMetricProfile(String metricProfile) {
            this.metricProfile = metricProfile;
        }

        public String getMetricOp() {
            return metricOp;
        }

        public void setMetricOp(String metricOp) {
            this.metricOp = metricOp;
        }

        public String getGroupType() {
            return groupType;
        }

        public void setGroupType(String groupType) {
            this.groupType = groupType;
        }

        public String getOp() {
            return op;
        }

        public void setOp(String op) {
            this.op = op;
        }

        public HashMap<String, ServGroupItem> getGroups() {
            return groups;
        }

        public void setGroups(HashMap<String, ServGroupItem> groups) {
            this.groups = groups;
        }

        public HashMap<String, String> getServiceIndex() {
            return serviceIndex;
        }

        public void setServiceIndex(HashMap<String, String> serviceIndex) {
            this.serviceIndex = serviceIndex;
        }

        public HashMap<String, String> getServiceOperations() {
            return serviceOperations;
        }

        public void setServiceOperations(HashMap<String, String> serviceOperations) {
            this.serviceOperations = serviceOperations;
        }

       public HashMap<String, String> getGroupOperations() {
            return groupOperations;
        }

        public void setGroupOperationss(HashMap<String, String> groupOperations) {
            this.groupOperations = groupOperations;
        }

    }

    /**
     * clears the list containing the AvProfileItem
     */
    public void clearProfiles() {
        this.list.clear();
    }

    /**
     * returns the profile operation as exists in the profile data field
     * profile_operation
     *
     * @param avProfile , the name of the aggregation profile name
     * @return , the profile operation
     */
    public String getTotalOp(String avProfile) {
        if (this.list.containsKey(avProfile)) {
            return this.list.get(avProfile).op;
        }

        return "";
    }

    /**
     * returns the metric operation as exists in the profile data field
     * metric_operation returns the metric operation as exists in the profile
     * data field metric_operation
     *
     * @param avProfile
     * @return
     */
    public String getMetricOp(String avProfile) {
        if (this.list.containsKey(avProfile)) {
            return this.list.get(avProfile).metricOp;
        }

        return "";
    }

    /**
     * Return the available Group Names of a profile
     *
     * @param avProfile , the profile's name
     * @return , a list with the groups (functions)
     */
    // Return the available Group Names of a profile
    public ArrayList<String> getProfileGroups(String avProfile) {

        if (this.list.containsKey(avProfile)) {
            ArrayList<String> result = new ArrayList<String>();
            Iterator<String> groupIterator = this.list.get(avProfile).groups.keySet().iterator();

            while (groupIterator.hasNext()) {
                result.add(groupIterator.next());
            }

            return result;
        }

        return null;
    }

    /**
     * Return the available group operation
     *
     * @param avProfile , the profile's name
     * @param groupName , the group
     * @return the operation corresponding to the group
     */
    public String getProfileGroupOp(String avProfile, String groupName) {
        if (this.list.containsKey(avProfile)) {
            if (this.list.get(avProfile).groups.containsKey(groupName)) {
                return this.list.get(avProfile).groups.get(groupName).op;
            }
        }

        return null;
    }

    /**
     * Return the available services for the group
     *
     * @param avProfile , the profile's name
     * @param groupName , the group
     * @return a list containing the services corresponding to the group
     */
    public ArrayList<String> getProfileGroupServices(String avProfile, String groupName) {
        if (this.list.containsKey(avProfile)) {
            if (this.list.get(avProfile).groups.containsKey(groupName)) {
                ArrayList<String> result = new ArrayList<String>();
                Iterator<String> srvIterator = this.list.get(avProfile).groups.get(groupName).services.keySet()
                        .iterator();

                while (srvIterator.hasNext()) {
                    result.add(srvIterator.next());
                }

                return result;
            }
        }

        return null;
    }

    public String getProfileGroupServiceOp(String avProfile, String groupName, String service) {

        if (this.list.containsKey(avProfile)) {
            if (this.list.get(avProfile).groups.containsKey(groupName)) {
                if (this.list.get(avProfile).groups.get(groupName).services.containsKey(service)) {
                    return this.list.get(avProfile).groups.get(groupName).services.get(service);
                }
            }
        }

        return null;
    }

    public ArrayList<String> getAvProfiles() {

        if (this.list.size() > 0) {
            ArrayList<String> result = new ArrayList<String>();
            Iterator<String> avpIterator = this.list.keySet().iterator();
            while (avpIterator.hasNext()) {
                result.add(avpIterator.next());
            }

            return result;

        }

        return null;
    }

    public String getProfileNamespace(String avProfile) {

        if (this.list.containsKey(avProfile)) {
            return this.list.get(avProfile).namespace;
        }

        return null;
    }

    public String getProfileMetricProfile(String avProfile) {

        if (this.list.containsKey(avProfile)) {
            return this.list.get(avProfile).metricProfile;
        }

        return null;
    }

    public String getProfileGroupType(String avProfile) {

        if (this.list.containsKey(avProfile)) {
            return this.list.get(avProfile).groupType;
        }

        return null;
    }

    public String getGroupByService(String avProfile, String service) {

        if (this.list.containsKey(avProfile)) {

            return this.list.get(avProfile).serviceIndex.get(service);

        }
        return null;

    }

    public boolean checkService(String avProfile, String service) {

        if (this.list.containsKey(avProfile)) {

            if (this.list.get(avProfile).serviceIndex.containsKey(service)) {
                return true;
            }

        }
        return false;

    }

    public void loadJson(File jsonFile) throws IOException {

        BufferedReader br = null;
        try {

            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser json_parser = new JsonParser();
            JsonElement j_element = json_parser.parse(br);
            readJson(j_element);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not open file:" + jsonFile.getName());
            throw ex;

        } catch (JsonParseException ex) {
            LOG.error("File is not valid json:" + jsonFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(br);
        }

    }

    public void loadJsonString(List<String> apsJson) throws IOException {

        try {

            JsonParser jsonParser = new JsonParser();
            JsonElement jRootElement = jsonParser.parse(apsJson.get(0));
            readJson(jRootElement);

        } catch (JsonParseException ex) {
            LOG.error("Contents are not valid json");
            throw ex;
        }

    }

    /**
     * reads from a JsonElement and stores the necessary information to the
     * AggregationProfileManager object fields
     *
     * @param j_element , a JsonElement containing the aggregations profiles data
     */
    public void readJson(JsonElement j_element) {
        JsonObject jRootObj = j_element.getAsJsonObject();

        // Create new entry for this availability profile
        AvProfileItem tmpAvp = new AvProfileItem();

        JsonArray apGroups = jRootObj.getAsJsonArray("groups");
        tmpAvp.id = jRootObj.get("id").getAsString();
        tmpAvp.name = jRootObj.get("name").getAsString();
        tmpAvp.namespace = jRootObj.get("namespace").getAsString();
        tmpAvp.metricProfile = jRootObj.get("metric_profile").getAsJsonObject().get("name").getAsString();
        tmpAvp.metricOp = jRootObj.get("metric_operation").getAsString();
        tmpAvp.groupType = jRootObj.get("endpoint_group").getAsString();
        tmpAvp.op = jRootObj.get("profile_operation").getAsString();

        for (JsonElement item : apGroups) {
            // service name
            JsonObject itemObj = item.getAsJsonObject();
            String itemName = itemObj.get("name").getAsString();
            String itemOp = itemObj.get("operation").getAsString();
            tmpAvp.groupOperations.put(itemName, itemOp);
            JsonArray itemServices = itemObj.get("services").getAsJsonArray();
            tmpAvp.insertGroup(itemName, itemOp);

            for (JsonElement subItem : itemServices) {
                JsonObject subObj = subItem.getAsJsonObject();
                String serviceName = subObj.get("name").getAsString();
                String serviceOp = subObj.get("operation").getAsString();
                tmpAvp.insertService(itemName, serviceName, serviceOp);
            }

        }
        buildExtraData(tmpAvp);

        // Add profile to the list
        this.list.put(tmpAvp.name, tmpAvp);
        this.profileNameIdMap.put(tmpAvp.id, tmpAvp.name);

    }

    /**
     * populates extra information by processing the parsed aggregation profile
     * data
     *
     * @param item, the AvProfileItem created while parsing the aggregation
     * profile data input
     */
    private void buildExtraData(AvProfileItem item) {

        HashMap<String, String> groupOperations = new HashMap<>();

        HashMap<String, String> serviceOperations = new HashMap<>();
        HashMap<String, ArrayList<String>> serviceFunctions = new HashMap<>();

        for (String group : item.groups.keySet()) {
            AvProfileItem.ServGroupItem servGroupItem = item.groups.get(group);

            if (groupOperations.containsKey(group)) {
                groupOperations.put(group, servGroupItem.op);
            }

            for (Entry<String, String> entry : servGroupItem.services.entrySet()) {
                serviceOperations.put(entry.getKey(), entry.getValue());

                ArrayList<String> functions = new ArrayList<>();
                if (serviceFunctions.containsKey(entry.getKey())) {
                    functions = serviceFunctions.get(entry.getKey());
                }
                functions.add(group);
                serviceFunctions.put(entry.getKey(), functions);
            }
        }
        item.serviceOperations = serviceOperations;
        // item.serviceFunctions = serviceFunctions;
    }

    /**
     * Return the available function that correspond to the service input
     *
     * @param service a service
     * @return a function correspondig to the service
     */
    public String retrieveFunctionsByService(String service) {

        AvProfileItem item = getAvProfileItem();
        return item.serviceIndex.get(service);
    }

    /**
     * Return a map containing all the pairs of service , operation that appear
     * in the aggregation profile data
     *
     * @return the map containing the service , operation pairs
     */
    public HashMap<String, String> retrieveServiceOperations() {

        AvProfileItem item = getAvProfileItem();
        return item.serviceOperations;
    }

    /**
     * Return a map containing all the pairs of service , group (function) that
     * appear in the aggregation profile data
     *
     * @return a map containing service, group (function) pairs
     */
    public HashMap<String, String> retrieveServiceFunctions() {

        AvProfileItem item = getAvProfileItem();
        return item.serviceIndex;
    }

    /**
     * Checks the service, group (function) pairs for the existance of the
     * service input
     *
     * @param service a service
     * @return true if the service exist in the map , false elsewhere
     */
    public boolean checkServiceExistance(String service) {
        AvProfileItem item = getAvProfileItem();

        return item.serviceIndex.keySet().contains(service);
    }

    public AvProfileItem getAvProfileItem() {
        String name = this.list.keySet().iterator().next();
        return this.list.get(name);

    }

    /**
     * Return the profile's metric operation as exist in the aggregation profile
     * data field : metric_operation
     *
     * @return the profile's metric operation
     */
    public String getMetricOpByProfile() {
        AvProfileItem item = getAvProfileItem();

        return item.metricOp;

    }

    /**
     * Return a map containing all the pairs of group (function) , operation
     * that appear in the aggregation profile data
     *
     * @return the map containing all the pairs of group (function), operation
     */
    public HashMap<String, String> retrieveGroupOperations() {

        AvProfileItem item = getAvProfileItem();

        return item.groupOperations;
    }

    /**
     * Return the operation that correspond to the group input
     *
     * @param group , a group (function)
     * @return the operation corresponding to the group
     */
    public String retrieveGroupOperation(String group) {
        AvProfileItem item = getAvProfileItem();

        return item.groups.get(group).op;
    }

    /**
     * Return the profile's profile_operation
     *
     * @return the profile's operation
     */
    public String retrieveProfileOperation() {
        AvProfileItem item = getAvProfileItem();

        return item.op;
    }

}
