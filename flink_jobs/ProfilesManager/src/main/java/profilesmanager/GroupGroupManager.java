package profilesmanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import argo.avro.GroupGroup;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 *
 *
 * GroupGroupManager class implements objects that store the information parsed
 * from a json object containing topology group-endpoint profile data or loaded
 * from an avro file
 *
 * The GroupGroupManager keeps info of a list of GroupItem objects each one
 * representing the current topology information that corresponds to a specific
 * client's report or the topology information that is valid for a given date ,
 * for a specific client's report
 *
 * Also stores a list of GroupItem objects that are filtered on the tags
 * information, given some criteria on the tags
 */
public class GroupGroupManager implements Serializable {

    static Logger log = Logger.getLogger(GroupGroupManager.class.getName());

    private ArrayList<GroupItem> list;
    private ArrayList<GroupItem> fList;

    //* A GroupItem class implements an object containing info of an group endpoint included in the  topology
    protected class GroupItem implements Serializable {

        String type; // type of group
        String group; // name of the group
        String subgroup; // name of sub-group
        HashMap<String, String> tags; // Tag list

        public GroupItem() {
            // Initializations
            this.type = "";
            this.group = "";
            this.subgroup = "";
            this.tags = new HashMap<String, String>();
        }

        public GroupItem(String type, String group, String subgroup, HashMap<String, String> tags) {
            this.type = type;
            this.group = group;
            this.subgroup = subgroup;
            this.tags = tags;

        }

    }

    public GroupGroupManager() {
        this.list = new ArrayList<GroupItem>();
        this.fList = new ArrayList<GroupItem>();
    }

    /**
     * Inserts into the list a new EndpointItem , that contains the information
     * of a group endpoint of the topology
     *
     * @param type the type of the group group
     * @param group the group of the group group
     * @param subgroup the subgroup of the group group
     * @param tags the tags of the endpoint group
     * @return 0 if the item is inserted successfully to the list of the
     * GroupItems
     */
    public int insert(String type, String group, String subgroup, HashMap<String, String> tags) {
        GroupItem new_item = new GroupItem(type, group, subgroup, tags);
        this.list.add(new_item);
        return 0; // All good
    }

    /**
     * Finds in the filtered GroupItem fList a GroupItem with specific type.
     * subgroup and return the tags if it is found
     *
     * @param type a type
     * @param subgroup a subgroup
     * @return a map of tags for a specific group group or null if there are no
     * tags
     */
    public HashMap<String, String> getGroupTags(String type, String subgroup) {
        for (GroupItem item : this.fList) {
            if (item.type.equals(type) && item.subgroup.equals(subgroup)) {
                return item.tags;
            }
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
     * Returns the group corresponding to the specific type, subgroup info
     *
     * @param type the type
     * @param subgroup the subgroup
     * @return a group group with the specific type, subgroup
     */
    public String getGroup(String type, String subgroup) {
        for (GroupItem item : this.fList) {
            if (item.type.equals(type) && item.subgroup.equals(subgroup)) {
                return item.group;
            }
        }

        return null;
    }

    /**
     * Clear the filtered fList and initialize with all the GroupItems the
     * list includes
     */
    public void unfilter() {
        this.fList.clear();
        for (GroupItem item : this.list) {
            this.fList.add(item);
        }
    }

    /**
     * Applies filter on the tags of the GroupItems included in the list, based
     * on a map of criteria each criteria containing a pair of the tags field
     * name and the value it searches . e.g ("monitored","1") searches tags with
     * the specific pair of criteria
     *
     * @param fTags a map of criteria
     */
    public void filter(TreeMap<String, String> fTags) {
        this.fList.clear();
        boolean trim;
        for (GroupItem item : this.list) {
            trim = false;
            HashMap<String, String> itemTags = item.tags;
            for (Entry<String, String> fTagItem : fTags.entrySet()) {

                if (itemTags.containsKey(fTagItem.getKey())) {
                    if (itemTags.get(fTagItem.getKey()).equalsIgnoreCase(fTagItem.getValue()) == false) {
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
     * Checks the filtered list if group group with a specific subgroup exists
     *
     * @param subgroup
     * @return true if the group group exists, else elsewhere
     */
    public boolean checkSubGroup(String subgroup) {
        for (GroupItem item : fList) {
            if (item.subgroup.equals(subgroup)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Loads groups of groups information from an avro file
     * <p>
     * This method loads groups of groups information contained in an .avro file
     * with specific avro schema.
     *
     * <p>
     * The following fields are expected to be found in each avro row:
     * <ol>
     * <li>type: string (describes the type of grouping)</li>
     * <li>group: string</li>
     * <li>subgroup: string</li>
     * <li>tags: hashmap (contains a map of arbitrary key values)</li>
     * </ol>
     *
     * @param avroFile a File object of the avro file that will be opened
     * @throws IOException if there is an error during opening of the avro file
     */
    public void loadAvro(File avroFile) throws IOException {

        // Prepare Avro File Readers
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

            // Grab avro schema
            Schema avroSchema = dataFileReader.getSchema();

            // Generate 1st level generic record reader (rows)
            GenericRecord avroRow = new GenericData.Record(avroSchema);

            // For all rows in file repeat
            while (dataFileReader.hasNext()) {
                // read the row
                avroRow = dataFileReader.next(avroRow);
                HashMap<String, String> tagMap = new HashMap<String, String>();

                // Generate 2nd level generic record reader (tags)
                HashMap tags = (HashMap) avroRow.get("tags");
                if (tags != null) {
                    for (Object item : tags.keySet()) {
                        tagMap.put(item.toString(), tags.get(item).toString());
                    }
                }

                // Grab 1st level mandatory fields
                String type = avroRow.get("type").toString();
                String group = avroRow.get("group").toString();
                String subgroup = avroRow.get("subgroup").toString();

                // Insert data to list
                this.insert(type, group, subgroup, tagMap);

            } // end of avro rows

            this.unfilter();

            dataFileReader.close();

        } catch (IOException ex) {
            log.error("Could not open avro file:" + avroFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(dataFileReader);
        }

    }

    /**
     * Loads group of group information from a list of GroupGroup objects
     *
     */
    @SuppressWarnings("unchecked")
    public void loadFromList(List<GroupGroup> ggp) {

        // For each group of groups record
        for (GroupGroup item : ggp) {
            String type = item.getType();
            String group = item.getGroup();
            String subgroup = item.getSubgroup();

            HashMap<String, String> tagMap = new HashMap<String, String>();
            HashMap<String, String> tags = (HashMap<String, String>) item.getTags();

            if (tags != null) {
                for (String key : tags.keySet()) {
                    tagMap.put(key, tags.get(key));
                }
            }

            // Insert data to list
            this.insert(type, group, subgroup, tagMap);
        }

        this.unfilter();

    }

    public void loadGroupGroupProfile(JsonArray element) throws IOException {
        GroupGroup[] groupGroups = readJson(element);
        loadFromList(Arrays.asList(groupGroups));
    }

    /**
     * reads from a JsonElement array and stores the necessary information to
     * the GroupGroup objects and add them to the list
     *
     * @param jElement , a JsonElement containing the topology group endpoint
     * profiles data
     * @return
     */
    public GroupGroup[] readJson(JsonArray jElement) {
        List<GroupGroup> results = new ArrayList<>();

        for (int i = 0; i < jElement.size(); i++) {
            JsonObject jItem = jElement.get(i).getAsJsonObject();
            String group = jItem.get("group").getAsString();
            String gType = jItem.get("type").getAsString();
            String subgroup = jItem.get("subgroup").getAsString();
            JsonObject jTags = jItem.get("tags").getAsJsonObject();
            Map<String, String> tags = new HashMap<String, String>();
            for (Entry<String, JsonElement> kv : jTags.entrySet()) {
                tags.put(kv.getKey(), kv.getValue().getAsString());
            }
            GroupGroup gg = new GroupGroup(gType, group, subgroup, tags);
            results.add(gg);
        }

        GroupGroup[] rArr = new GroupGroup[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    public ArrayList<GroupItem> getList() {
        return list;
    }

    public void setList(ArrayList<GroupItem> list) {
        this.list = list;
    }

    public ArrayList<GroupItem> getfList() {
        return fList;
    }

    public void setfList(ArrayList<GroupItem> fList) {
        this.fList = fList;
    }

}
