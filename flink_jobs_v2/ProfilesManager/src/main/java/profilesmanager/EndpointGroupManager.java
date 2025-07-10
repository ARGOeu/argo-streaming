package profilesmanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import argo.avro.GroupEndpoint;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.Arrays;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.json.simple.JSONObject;

/**
 *
 *
 * EndpointGroupManager class implements objects that store the information
 * parsed from a json object containing topology group-endpoint profile data or
 * loaded from an avro file
 *
 * The EndpointGroupManager keeps info of a list of EndpointItem objects each
 * one representing the current topology information that corresponds to a
 * specific client's report or the topology information that is valid for a
 * given date , for a specific client's report
 *
 * Also stores a list of EndpointItem objects that are filtered on the tags
 * information, given some criteria on the tags
 */
public class EndpointGroupManager implements Serializable {

    private static final Logger LOG = Logger.getLogger(EndpointGroupManager.class.getName());

    private ArrayList<EndpointItem> list; // the list of EndpointItems that are included in the profile data
    private ArrayList<EndpointItem> fList; // the list of the filter EndpointItems that correspond to the criteria

    private Map<String, Map<String, EndpointItem>> topologyGrouplist;
    private Map<String, ArrayList<EndpointItem>> groupIndex;
    private String defaultType = null;

    //* A EndpointItem class implements an object containing info of an group endpoint included in the  topology
    public class EndpointItem implements Serializable { // the object

        String type; // type of group
        String group; // name of the group
        String service; // type of the service
        String hostname; // name of host
        HashMap<String, String> tags; // Tag list

        public EndpointItem() {
            // Initializations
            this.type = "";
            this.group = "";
            this.service = "";
            this.hostname = "";
            this.tags = new HashMap<String, String>();
        }

        public EndpointItem(String type, String group, String service, String hostname, HashMap<String, String> tags) {
            this.type = type;
            this.group = group;
            this.service = service;
            this.hostname = hostname;
            this.tags = tags;

        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 17 * hash + Objects.hashCode(this.type);
            hash = 17 * hash + Objects.hashCode(this.group);
            hash = 17 * hash + Objects.hashCode(this.service);
            hash = 17 * hash + Objects.hashCode(this.hostname);
            hash = 17 * hash + Objects.hashCode(this.tags);
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
            final EndpointItem other = (EndpointItem) obj;
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }
            if (!Objects.equals(this.group, other.group)) {
                return false;
            }
            if (!Objects.equals(this.service, other.service)) {
                return false;
            }
            if (!Objects.equals(this.hostname, other.hostname)) {
                return false;
            }
            if (!Objects.equals(this.tags, other.tags)) {
                return false;
            }
            return true;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public HashMap<String, String> getTags() {
            return tags;
        }

        public void setTags(HashMap<String, String> tags) {
            this.tags = tags;
        }

        @Override
        public String toString() {
            return type + "," + group + "," + service + "," + hostname;
        }

    }

    public EndpointGroupManager() {
        this.list = new ArrayList<>();
        this.fList = new ArrayList<>();

        this.groupIndex = new HashMap<String, ArrayList<EndpointItem>>();
        this.topologyGrouplist = new HashMap<>();
    }

    /**
     * Inserts into the list a new EndpointItem , that contains the information
     * of a group endpoint of the topology
     *
     * @param type the type of the endpoint group
     * @param group the group of the endpoint group
     * @param service the service of the endpoint group
     * @param hostname the hostname of the endpoint group
     * @param tags the tags of the endpoint group
     * @return 0 if the item is inserted successfully to the list of the
     * EndpointItems
     */
    public int insert(String type, String group, String service, String hostname, HashMap<String, String> tags) {
        EndpointItem new_item = new EndpointItem(type, group, service, hostname, tags);
        this.list.add(new_item);
        return 0; // All good
    }

    /**
     * Checks the filtered list if group endpoint with a hostname , service
     * exists
     *
     * @param hostname
     * @param service
     * @return true if the group endpoint exists, else elsewhere
     */
    public boolean checkEndpoint(String hostname, String service) {

        for (EndpointItem item : fList) {
            if (item.hostname.equals(hostname) && item.service.equals(service)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the list of groups each one containing the group endpoint with
     * the specific info
     *
     * @param type the type
     * @param hostname the hostname
     * @param service the service
     * @return a list of groups that contain the group endpoint with the
     * specific type, hostname, service
     */
    public ArrayList<String> getGroup(String type, String hostname, String service) {

        ArrayList<String> results = new ArrayList<String>();

        for (EndpointItem item : fList) {
            if (item.type.equals(type) && item.hostname.equals(hostname) && item.service.equals(service)) {
                if (!results.contains(item.group)) {
                    results.add(item.group);
                }
            }
        }

        return results;
    }

    /**
     * Searches the tags of a group endpoint with the specific group, type,
     * hostname, service and if the tags map contains keys with "info_" in them
     * then it returns the values combinations
     *
     * @param group the group
     * @param type the type
     * @param hostname the hostname
     * @param service the service
     * @return a string where all values of a "info_" tag are combined and
     * represented
     */
    public String getInfo(String group, String type, String hostname, String service) {
        String info = "";
        HashMap<String, String> tags = this.getGroupTags(group, type, hostname, service);

        if (tags == null) {
            return info;
        }
        JsonObject job = new JsonObject();
        for (String tName : tags.keySet()) {

            if (tName.startsWith("info_")) {
                String infoName = tName.replaceFirst("info_", "");

                String value = tags.get(tName);

                if (!value.equalsIgnoreCase("")) {

                    job.addProperty(infoName, value);
                }
            }
        }
        Gson gson = new Gson();
        return gson.toJson(job);
    }

    /**
     * Finds in the filtered EndpointItem fList a EndpointItem with specific
     * group, type. service, hostname and return the tags if it is found
     *
     * @param group a group
     * @param type a type
     * @param hostname a hostname
     * @param service a service
     * @return a map of tags for a specific group endpoint or null if there are
     * no tags
     */
    public HashMap<String, String> getGroupTags(String group, String type, String hostname, String service) {

        for (EndpointItem item : fList) {
            if (item.group.equals(group) && item.type.equals(type) && item.hostname.equals(hostname) && item.service.equals(service)) {
                return item.tags;
            }
        }

        return null;
    }
    
    /**
     * Finds in the filtered EndpointItem fList a EndpointItem with specific
     * group, type. service, hostname and tag and returns it's value
     *
     * @param group a group
     * @param type a type
     * @param hostname a hostname
     * @param service a service
     * @param tag this is the tag name
     * @return return tag value
     **/
    public String getTag(String group, String type, String hostname, String service, String tag) {
    	HashMap<String, String> results = this.getGroupTags(group, type, hostname, service);
    	if (results != null){
    		return results.get(tag);
    	}
    	
    	return null;
    }

    /**
     * counts the items in the filtered list
     *
     * @return
     */
    public int count() {
        return this.fList.size();
    }

    /**
     * Clear the filtered fList and initialize with all the EndpointItems the
     * list includes
     */
    public void unfilter() {
        this.fList.clear();
        for (EndpointItem item : this.list) {
            this.fList.add(item);
        }
    }

    /**
     * Applies filter on the tags of the EndpointItems included in the list,
     * based on a map of criteria each criteria containing a pair of the tags
     * field name and the value it searches . e.g ("monitored","1") searches
     * tags with the specific pair of criteria
     *
     * @param fTags a map of criteria
     */
    public void filter(TreeMap<String, String> fTags) {
        this.fList.clear();
        boolean trim;
        for (EndpointItem item : this.list) {
            trim = false;
            HashMap<String, String> itemTags = item.tags;
            for (Entry<String, String> fTagItem : fTags.entrySet()) {

                if (itemTags.containsKey(fTagItem.getKey())) {
                    // First Check binary tags as Y/N 0/1

                    if (fTagItem.getValue().equalsIgnoreCase("y") || fTagItem.getValue().equalsIgnoreCase("n")) {
                        String binValue = "";
                        if (fTagItem.getValue().equalsIgnoreCase("y")) {
                            binValue = "1";
                        }
                        if (fTagItem.getValue().equalsIgnoreCase("n")) {
                            binValue = "0";
                        }

                        if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(binValue) == false) {
                            trim = true;
                        }
                    } else if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(fTagItem.getValue()) == false) {
                        trim = true;
                    }

                }
            }

            if (trim == false) {
                fList.add(item);
            }
        }
    }

    /**
     * Loads endpoint grouping information from an avro file
     * <p>
     * This method loads endpoint grouping information contained in an .avro
     * file with specific avro schema.
     *
     * <p>
     * The following fields are expected to be found in each avro row:
     * <ol>
     * <li>type: string (describes the type of grouping)</li>
     * <li>group: string</li>
     * <li>service: string</li>
     * <li>hostname: string</li>
     * <li>tags: hashmap (contains a map of arbitrary key values)</li>
     * </ol>
     *
     * @param avroFile a File object of the avro file that will be opened
     * @throws IOException if there is an error during opening of the avro file
     */
    @SuppressWarnings("unchecked")
    public void loadAvro(File avroFile) throws IOException {

        // Prepare Avro File Readers
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

            // Grab Avro schema
            Schema avroSchema = dataFileReader.getSchema();

            // Generate 1st level generic record reader (rows)
            GenericRecord avroRow = new GenericData.Record(avroSchema);

            // For all rows in file repeat
            while (dataFileReader.hasNext()) {
                // read the row
                avroRow = dataFileReader.next(avroRow);
                HashMap<String, String> tagMap = new HashMap<String, String>();

                // Generate 2nd level generic record reader (tags)
                HashMap<Utf8, String> tags = (HashMap<Utf8, String>) (avroRow.get("tags"));

                if (tags != null) {
                    for (Utf8 item : tags.keySet()) {
                        tagMap.put(item.toString(), String.valueOf(tags.get(item)));
                    }
                }

                // Grab 1st level mandatory fields
                String type = avroRow.get("type").toString();
                String group = avroRow.get("group").toString();
                String service = avroRow.get("service").toString();
                String hostname = avroRow.get("hostname").toString();

                // Insert data to list
                this.insert(type, group, service, hostname, tagMap);

                this.insertTopologyGroup(type, group, service, hostname, tagMap);
                defaultType = type;

//                populateTopologyEndpoint(hostname, service, type, group);
            } // end of avro rows

            this.unfilter();

        } catch (IOException ex) {
            LOG.error("Could not open avro file:" + avroFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(dataFileReader);
        }

    }

    public ArrayList<EndpointItem> getList() {
        return this.list;
    }

    public ArrayList<EndpointItem> getfList() {
        return fList;
    }

    public void setfList(ArrayList<EndpointItem> fList) {
        this.fList = fList;
    }

    /**
     * Loads information from a list of EndpointGroup objects
     *
     */
    @SuppressWarnings("unchecked")
    public void loadFromList(List<GroupEndpoint> egp) {

        this.list.clear();
        this.fList.clear();
        // For each endpoint group record
        for (GroupEndpoint item : egp) {
            String type = item.getType();
            String group = item.getGroup();
            String service = item.getService();
            String hostname = item.getHostname();
            HashMap<String, String> tagMap = new HashMap<String, String>();
            HashMap<String, String> tags = (HashMap<String, String>) item.getTags();
            if (tags != null) {
                for (String key : tags.keySet()) {
                    tagMap.put(key, tags.get(key));
                }
            }

            // Insert data to list
            this.insert(type, group, service, hostname, tagMap);

            this.insertTopologyGroup(type, group, service, hostname, tagMap);
            defaultType = type;
        }

        this.unfilter();

    }

    public void loadGroupEndpointProfile(JsonArray element) throws IOException {
        GroupEndpoint[] groupEndpoints = readJson(element);
        loadFromList(Arrays.asList(groupEndpoints));
    }

    /**
     * reads from a JsonElement array and stores the necessary information to
     * the GroupEndpoint objects and add them to the list
     *
     * @param jElement , a JsonElement containing the topology group endpoint
     * profiles data
     * @return
     */
    public GroupEndpoint[] readJson(JsonArray jElement) {
        List<GroupEndpoint> results = new ArrayList<>();

        for (int i = 0; i < jElement.size(); i++) {
            JsonObject jItem = jElement.get(i).getAsJsonObject();
            String group = jItem.get("group").getAsString();
            String gType = jItem.get("type").getAsString();
            String service = jItem.get("service").getAsString();
            String hostname = jItem.get("hostname").getAsString();
            JsonObject jTags = jItem.get("tags").getAsJsonObject();
            Map<String, String> tags = new HashMap<String, String>();
            for (Entry<String, JsonElement> kv : jTags.entrySet()) {
                tags.put(kv.getKey(), kv.getValue().getAsString());
            }
            GroupEndpoint ge = new GroupEndpoint(gType, group, service, hostname, tags);
            results.add(ge);
        }

        GroupEndpoint[] rArr = new GroupEndpoint[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * ***********************************
     */
    public Set<String> getEndpointSet() {
        Set<String> curItems = new HashSet<String>();
        for (String groupKey : this.groupIndex.keySet()) {
            ArrayList<EndpointItem> eList = this.groupIndex.get(groupKey);
            for (EndpointItem item : eList) {
                curItems.add(item.toString());
            }
        }
        return curItems;
    }

    public ArrayList<String> getGroupList() {
        ArrayList<String> results = new ArrayList<String>();
        results.addAll(this.groupIndex.keySet());
        return results;
    }

    public ArrayList<String> compareToBeRemoved(EndpointGroupManager egp) {

        ArrayList<String> results = new ArrayList<String>();

        Set<String> curItems = this.getEndpointSet();
        Set<String> futurItems = egp.getEndpointSet();

        // lost items is cur items minus future set
        curItems.removeAll(futurItems);

        results.addAll(curItems);

        return results;
    }

    public int insertTopologyGroup(String type, String group, String service, String hostname, HashMap<String, String> tags) {
        EndpointItem itemNew = new EndpointItem(type, group, service, hostname, tags);
        String key = type + "|" + hostname + "|" + service;
        if (!topologyGrouplist.containsKey(key)) {
            Map<String, EndpointItem> subList = new HashMap<String, EndpointItem>();
            subList.put(group, itemNew);
            topologyGrouplist.put(key, subList);

        } else {
            Map<String, EndpointItem> subList = topologyGrouplist.get(key);
            subList.put(group, itemNew);
        }
        // Add item to the secondary group index
        if (!groupIndex.containsKey(group)) {
            groupIndex.put(group, new ArrayList<EndpointItem>(Arrays.asList(itemNew)));
        } else {
            groupIndex.get(group).add(itemNew);
        }

        return 0; // All good
    }

    public boolean checkEndpointGroup(String hostname, String service) {

        String key = defaultType + "|" + hostname + "|" + service;
        return topologyGrouplist.containsKey(key);
    }

    public ArrayList<String> getGroupFull(String type, String hostname, String service) {

        String key = type + "|" + hostname + "|" + service;
        Map<String, EndpointItem> sublist = topologyGrouplist.get(key);
        if (sublist != null) {
            return new ArrayList<String>(topologyGrouplist.get(key).keySet());
        }

        return new ArrayList<String>();

    }

    public Iterator<EndpointItem> getGroupIter(String group) {
        ArrayList<EndpointItem> list = groupIndex.get(group);
        if (list != null) {
            return list.iterator();
        }

        return null;
    }

    public ArrayList<String> getGroup(String hostname, String service) {

        String key = defaultType + "|" + hostname + "|" + service;
        Map<String, EndpointItem> sublist = topologyGrouplist.get(key);
        if (sublist != null) {
            return new ArrayList<String>(topologyGrouplist.get(key).keySet());
        }

        return new ArrayList<String>();

    }

    public ArrayList<String> getGroupAllTopo(String hostname, String service) {
        String keyPart = hostname + "|" + service;

        for (String key : topologyGrouplist.keySet()) {
            if (key.contains(keyPart)) {
                Map<String, EndpointItem> sublist = topologyGrouplist.get(key);
                if (sublist != null) {
                    return new ArrayList<String>(sublist.keySet());
                }
            }
        }

        return new ArrayList<String>();
    }
}
