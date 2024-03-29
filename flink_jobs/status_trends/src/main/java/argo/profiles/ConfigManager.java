package argo.profiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 *
 * ConfigManager class implements objects that store the information parsed from
 * a json object containing the profile data of a tenant's report or loaded from
 * a json file
 */

public class ConfigManager {

    private static final Logger LOG = Logger.getLogger(ConfigManager.class.getName());

    public String id; // report uuid reference
    public String report;
    public String tenant;
    public String egroup; // endpoint group
    public String ggroup; // group of groups
    public String weight; // weight factor type
    public TreeMap<String, String> egroupTags;
    public TreeMap<String, String> ggroupTags;
    public TreeMap<String, String> mdataTags;
    private Threshold threshold;
    ArrayList<Profiles> profiles;

    /**
     * Constructor to a ConfigManager object to store the tenant's report
     * information
     */
    public ConfigManager() {
        this.report = null;
        this.id = null;
        this.tenant = null;
        this.egroup = null;
        this.ggroup = null;
        this.weight = null;
        this.egroupTags = new TreeMap<String, String>();
        this.ggroupTags = new TreeMap<String, String>();
        this.mdataTags = new TreeMap<String, String>();
        this.profiles = new ArrayList<>();

    }

    /**
     * Clears the stored data of a ConfigManager object
     */
    public void clear() {
        this.id = null;
        this.report = null;
        this.tenant = null;
        this.egroup = null;
        this.ggroup = null;
        this.weight = null;
        this.threshold = null;
        this.egroupTags.clear();
        this.ggroupTags.clear();
        this.mdataTags.clear();
        this.profiles.clear();

    }

    public String getReportID() {
        return id;
    }

    public String getReport() {
        return report;
    }

    public String getTenant() {
        return tenant;
    }

    public String getEgroup() {
        return egroup;
    }

    /**
     * loads from a json file that contain the tenant's report , and stores the
     * info to the corresponding fields
     *
     * @param jsonFile
     * @throws IOException
     */
    public void loadJson(File jsonFile) throws IOException {
        // Clear data
        this.clear();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser jsonParser = new JsonParser();
            JsonElement jElement = jsonParser.parse(br);
            readJson(jElement);
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

    /**
     * Loads Report config information from a config json string
     *
     */
    public void loadJsonString(List<String> confJson) throws JsonParseException {
        // Clear data
        this.clear();

        try {

            JsonParser jsonParser = new JsonParser();
            // Grab the first - and only line of json from ops data
            JsonElement jElement = jsonParser.parse(confJson.get(0));
            readJson(jElement);

        } catch (JsonParseException ex) {
            LOG.error("Not valid json contents");
            throw ex;
        }

    }

    /**
     * reads from a JsonElement array and stores the necessary information to
     * the ConfigManager objects and add them to the list
     *
     * @param jElement , a JsonElement containing the tenant's report data
     * @return
     */
    public void readJson(JsonElement jElement) {

        JsonObject jObj = jElement.getAsJsonObject();
        // Get the simple fields
        this.id = jObj.get("id").getAsString();
        this.tenant = jObj.get("tenant").getAsString();
        this.report = jObj.get("info").getAsJsonObject().get("name").getAsString();

        // get topology schema names
        JsonObject topoGroup = jObj.get("topology_schema").getAsJsonObject().getAsJsonObject("group");
        this.ggroup = topoGroup.get("type").getAsString();
        this.egroup = topoGroup.get("group").getAsJsonObject().get("type").getAsString();

        // optional weight filtering
        this.weight = "";
        if (jObj.has("weight")) {
            this.weight = jObj.get("weight").getAsString();
        }
        // Get compound fields
        JsonArray jTags = jObj.getAsJsonArray("filter_tags");

        // Iterate tags
        if (jTags != null) {
            for (JsonElement tag : jTags) {
                JsonObject jTag = tag.getAsJsonObject();
                String name = jTag.get("name").getAsString();
                String value = jTag.get("value").getAsString();
                String ctx = jTag.get("context").getAsString();
                if (ctx.equalsIgnoreCase("group_of_groups")) {
                    this.ggroupTags.put(name, value);
                } else if (ctx.equalsIgnoreCase("endpoint_groups")) {
                    this.egroupTags.put(name, value);
                } else if (ctx.equalsIgnoreCase("metric_data")) {
                    this.mdataTags.put(name, value);
                }

            }
        }

        JsonObject thresholdsObject = jObj.get("thresholds").getAsJsonObject();

        this.threshold = new Threshold(thresholdsObject.get("availability").getAsLong(), thresholdsObject.get("reliability").getAsLong(), thresholdsObject.get("uptime").getAsDouble(),
                thresholdsObject.get("unknown").getAsDouble(), thresholdsObject.get("downtime").getAsDouble());

        JsonArray profilesArray = jObj.get("profiles").getAsJsonArray();

        Iterator<JsonElement> profileIter = profilesArray.iterator();
        while (profileIter.hasNext()) {
            JsonObject profileObject = profileIter.next().getAsJsonObject();
            Profiles profile = new Profiles(profileObject.get("id").getAsString(), profileObject.get("name").getAsString(), profileObject.get("type").getAsString());
            profiles.add(profile);
        }

    }

    /**
     * Threshold class is an inner class that stores the info of the thresholds as described from the tenant's report
    */
    public class Threshold {

        private Long availability;
        private Long reliability;
        private Double uptime;
        private Double unknown;
        private Double downtime;

        public Threshold(Long availability, Long reliability, Double uptime, Double unknown, Double downtime) {
            this.availability = availability;
            this.reliability = reliability;
            this.uptime = uptime;
            this.unknown = unknown;
            this.downtime = downtime;
        }

        public Long getAvailability() {
            return availability;
        }

        public Long getReliability() {
            return reliability;
        }

        public Double getUptime() {
            return uptime;
        }

        public Double getUnknown() {
            return unknown;
        }

        public Double getDowntime() {
            return downtime;
        }

    }
/**
 * Profiles class is a class that stores the info of the profiles as described from the tenant's report
 */
    private class Profiles {

        private String id;
        private String name;
        private String type;

        public Profiles(String id, String name, String type) {
            this.id = id;
            this.name = name;
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

    }
/**
 * Returns the aggregation's report id , as described in the profiles of tenant's report
 * @return 
 */
    public String getAggregationReportId() {
        if (profiles != null) {
            for (Profiles profile : profiles) {
                if (profile.getType().equalsIgnoreCase(ProfileType.AGGREGATION.name())) {
                    return profile.id;
                }
            }
        }
        return null;
    }

 
    /**
     * Returns the metric's report id , as described in the profiles of tenant's report
     * @return 
     */
    public String getMetricReportId() {

        if (profiles != null) {
            for (Profiles profile : profiles) {
                if (profile.getType().equalsIgnoreCase(ProfileType.METRIC.name())) {
                    return profile.id;
                }
            }
        }
        return null;
    }

 /**
  * Returns the operation's report id , as described in the profiles of tenant's report
  * @return 
  */
    public String getOperationReportId() {
        if (profiles != null) {
            for (Profiles profile : profiles) {
                if (profile.getType().equalsIgnoreCase(ProfileType.OPERATIONS.name())) {
                    return profile.id;
                }
            }
        }
        return null;
    }

    public enum ProfileType {

        METRIC,
        AGGREGATION,
        OPERATIONS

    }
}
