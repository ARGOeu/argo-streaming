package profilesmanager;

import argo.avro.MetricProfile;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 *
 * MetricProfileManager class implements objects that store the information
 * parsed from a json object containing metric profile data or loaded from an
 * avro file
 *
 * The MetricProfileManager keeps info of a list of ProfileItems each one having
 * a name(profile), service, metric and a set of tags , also it convert string
 * operations and statuses to integer based on their position in the list
 * storage
 */
public class MetricProfileManager implements Serializable {

    private static final Logger LOG = Logger.getLogger(MetricProfileManager.class.getName());

    private ArrayList<ProfileItem> list; // a list of ProfileItem objects
    private Map<String, HashMap<String, ArrayList<String>>> index; //a map that stores as key a profile and as value a map of pairs of service (as key) and list of metrics (as value)

    /**
     *
     * A ProfileItem class implements an object containing info of a profile,
     * service, metric , tags
     */
    private class ProfileItem implements Serializable {

        String profile; // Name of the profile
        String service; // Name of the service type
        String metric; // Name of the metric
        HashMap<String, String> tags; // Tag list

        public ProfileItem() {
            // Initializations
            this.profile = "";
            this.service = "";
            this.metric = "";
            this.tags = new HashMap<String, String>();
        }

        public ProfileItem(String profile, String service, String metric, HashMap<String, String> tags) {
            this.profile = profile;
            this.service = service;
            this.metric = metric;
            this.tags = tags;
        }

        public String getProfile() {
            return profile;
        }

        public void setProfile(String profile) {
            this.profile = profile;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public HashMap<String, String> getTags() {
            return tags;
        }

        public void setTags(HashMap<String, String> tags) {
            this.tags = tags;
        }

    }

    /**
     * A constructor of a MetricProfileManager
     */

    public MetricProfileManager() {
        this.list = new ArrayList<ProfileItem>();
        this.index = new HashMap<String, HashMap<String, ArrayList<String>>>();
    }

    // Clear all profile data (both list and indexes)
    public void clear() {
        this.list = new ArrayList<ProfileItem>();
        this.index = new HashMap<String, HashMap<String, ArrayList<String>>>();
    }

    /**
     * Inserts a profile in the map
     *
     * @param profile, a profile
     * @return 0 if profile is added and -1 if the profile already exists
     */
    public int indexInsertProfile(String profile) {
        if (!index.containsKey(profile)) {
            index.put(profile, new HashMap<String, ArrayList<String>>());
            return 0;
        }
        return -1;
    }

    /**
     * Constructs a ProfileItem, add it at the list and insert into the index
     * map
     *
     * @param profile, a profile
     * @param service , a service
     * @param metric , a metric
     * @param tags , a map of tags
     */
    public void insert(String profile, String service, String metric, HashMap<String, String> tags) {
        ProfileItem tmpProfile = new ProfileItem(profile, service, metric, tags);
        this.list.add(tmpProfile);
        this.indexInsertMetric(profile, service, metric);
    }

    /**
     * Inserts a service at the index map , based on the profile as a key
     *
     * @param profile , a profile
     * @param service , a service
     * @return 0 if the service is added successfully at the index map, -1 if
     * the service already exists at the index map
     */
    public int indexInsertService(String profile, String service) {
        if (index.containsKey(profile)) {
            if (index.get(profile).containsKey(service)) {
                return -1;
            } else {
                index.get(profile).put(service, new ArrayList<String>());
                return 0;
            }

        }

        index.put(profile, new HashMap<String, ArrayList<String>>());
        index.get(profile).put(service, new ArrayList<String>());
        return 0;

    }

    /**
     * Inserts a metric at the index map , based on the profile and service as
     * keys
     *
     * @param profile, a profile
     * @param service , a service
     * @param metric , a metric
     * @return0 if the metric is added successfully at the index map, -1 if the
     * service already exists at the index map
     */
    public int indexInsertMetric(String profile, String service, String metric) {
        if (index.containsKey(profile)) {
            if (index.get(profile).containsKey(service)) {
                if (index.get(profile).get(service).contains(metric)) {
                    // Metric exists so no insertion
                    return -1;
                }
                // Metric doesn't exist and must be added
                index.get(profile).get(service).add(metric);
                return 0;
            } else {
                // Create the service and the metric
                index.get(profile).put(service, new ArrayList<String>());
                index.get(profile).get(service).add(metric);
                return 0;
            }

        }
        // No profile - service - metric so add them all
        index.put(profile, new HashMap<String, ArrayList<String>>());
        index.get(profile).put(service, new ArrayList<String>());
        index.get(profile).get(service).add(metric);
        return 0;

    }

    // Getter Functions
    public ArrayList<String> getProfileServices(String profile) {
        if (index.containsKey(profile)) {
            ArrayList<String> ans = new ArrayList<String>();
            ans.addAll(index.get(profile).keySet());
            return ans;
        }
        return null;

    }

    public ArrayList<String> getProfiles() {
        if (index.size() > 0) {
            ArrayList<String> ans = new ArrayList<String>();
            ans.addAll(index.keySet());
            return ans;
        }
        return null;
    }

    public ArrayList<String> getProfileServiceMetrics(String profile, String service) {
        if (index.containsKey(profile)) {
            if (index.get(profile).containsKey(service)) {
                return index.get(profile).get(service);
            }
        }
        return null;
    }

    /**
     * Checks if a combination of profile, service, metric exists in the index
     * map
     *
     * @param profile , a profile
     * @param service , a service
     * @param metric , a metric
     * @return true if the combination of profile , service, metric exists and
     * false if it does not
     */
    public boolean checkProfileServiceMetric(String profile, String service, String metric) {
        if (index.containsKey(profile)) {
            if (index.get(profile).containsKey(service)) {
                if (index.get(profile).get(service).contains(metric)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Loads metric profile information from an avro file
     * <p>
     * This method loads metric profile information contained in an .avro file
     * with specific avro schema.
     *
     * <p>
     * The following fields are expected to be found in each avro row:
     * <ol>
     * <li>profile: string</li>
     * <li>service: string</li>
     * <li>metric: string</li>
     * <li>[optional] tags: hashmap (contains a map of arbitrary key values)
     * </li>
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
                HashMap<Utf8, String> tags = (HashMap<Utf8, String>) (avroRow.get("tags"));

                if (tags != null) {
                    for (Utf8 item : tags.keySet()) {
                        tagMap.put(item.toString(), String.valueOf(tags.get(item)));
                    }
                }

                // Grab 1st level mandatory fields
                String profile = avroRow.get("profile").toString();
                String service = avroRow.get("service").toString();
                String metric = avroRow.get("metric").toString();

                // Insert data to list
                this.insert(profile, service, metric, tagMap);

            } // end of avro rows

            dataFileReader.close();

        } catch (IOException ex) {
            LOG.error("Could not open avro file:" + avroFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(dataFileReader);
        }

    }

    /**
     * Loads metric profile information from a list of MetricProfile objects
     *
     * @param mps , the list of MetricProfile objects
     */
    @SuppressWarnings("unchecked")
    public void loadFromList(List<MetricProfile> mps) {

        // For each metric profile object in list
        for (MetricProfile item : mps) {
            String profile = item.getProfile();
            String service = item.getService();
            String metric = item.getMetric();
            HashMap<String, String> tagMap = new HashMap<String, String>();
            HashMap<String, String> tags = (HashMap<String, String>) item.getTags();

            if (tags != null) {
                for (String key : tags.keySet()) {
                    tagMap.put(key, tags.get(key));
                }
            }

            // Insert data to list
            this.insert(profile, service, metric, tagMap);
        }

    }

    public void loadMetricProfile(JsonElement element) throws IOException {

        MetricProfile[] metrics = readJson(element);
        loadFromList(Arrays.asList(metrics));
    }

    /**
     * Reads from a json element and stores the information to an array of
     * MetricProfile objects
     *
     * @param jElement , a JsonElement containing the metric profile data info
     * @return
     */
    public MetricProfile[] readJson(JsonElement jElement) {
        List<MetricProfile> results = new ArrayList<MetricProfile>();

        JsonObject jRoot = jElement.getAsJsonObject();
        String profileName = jRoot.get("name").getAsString();
        JsonArray jElements = jRoot.get("services").getAsJsonArray();
        for (int i = 0; i < jElements.size(); i++) {
            JsonObject jItem = jElements.get(i).getAsJsonObject();
            String service = jItem.get("service").getAsString();
            JsonArray jMetrics = jItem.get("metrics").getAsJsonArray();
            for (int j = 0; j < jMetrics.size(); j++) {
                String metric = jMetrics.get(j).getAsString();

                Map<String, String> tags = new HashMap<String, String>();
                MetricProfile mp = new MetricProfile(profileName, service, metric, tags);
                results.add(mp);
            }

        }

        MetricProfile[] rArr = new MetricProfile[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }

    /**
     * Checks if a combination of service, metric exists ing the list of
     * ProfileItems
     *
     * @param service, a service
     * @param metric, a metric
     * @return true if the combination of a service, metric exists in the
     * ProfileItem list or false if it does not
     */
    public boolean containsMetric(String service, String metric) {

        for (ProfileItem profIt : list) {
            if (profIt.service.equals(service) && profIt.metric.equals(metric)) {
                return true;
            }
        }
        return false;
    }

    public ArrayList<ProfileItem> getList() {
        return list;
    }

    public void setList(ArrayList<ProfileItem> list) {
        this.list = list;
    }

    public Map<String, HashMap<String, ArrayList<String>>> getIndex() {
        return index;
    }

    public void setIndex(Map<String, HashMap<String, ArrayList<String>>> index) {
        this.index = index;
    }

}
