package argo.profiles;

import argo.avro.MetricProfile;
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

public class MetricProfileManager implements Serializable {
    
    private static final Logger LOG = Logger.getLogger(MetricProfileManager.class.getName());
    
    private ArrayList<ProfileItem> list;
    private Map<String, HashMap<String, ArrayList<String>>> index;
    private HashMap<Integer, String > profileNameIdMap;
    
    private class ProfileItem implements Serializable{

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
    
    public MetricProfileManager() {
        this.list = new ArrayList<ProfileItem>();
        this.index = new HashMap<String, HashMap<String, ArrayList<String>>>();
    }

    // Clear all profile data (both list and indexes)
    public void clear() {
        this.list = new ArrayList<ProfileItem>();
        this.index = new HashMap<String, HashMap<String, ArrayList<String>>>();
    }

    // Indexed List Functions
    public int indexInsertProfile(String profile) {
        if (!index.containsKey(profile)) {
            index.put(profile, new HashMap<String, ArrayList<String>>());
            return 0;
        }
        return -1;
    }
    
    public void insert(String profile, String service, String metric, HashMap<String, String> tags) {
        ProfileItem tmpProfile = new ProfileItem(profile, service, metric, tags);
        this.list.add(tmpProfile);
        this.indexInsertMetric(profile, service, metric);
    }
    
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

    public void loadMetricProfile(JsonElement element) throws IOException, org.json.simple.parser.ParseException {
        MetricProfile[] metrics = readJson(element);
        loadFromList(Arrays.asList(metrics));
    }
    
    public MetricProfile[] readJson(JsonElement jElement) {
        List<MetricProfile> results = new ArrayList<MetricProfile>();
//		if (!this.data.containsKey(ApiResource.METRIC)) {
//			MetricProfile[] rArr = new MetricProfile[results.size()]; 
//			rArr = results.toArray(rArr);
//			return rArr;
//		}
//			

//		String content = this.data.get(ApiResource.METRIC);
//		JsonParser jsonParser = new JsonParser();
//		JsonElement jElement = jsonParser.parse(content);
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
    
    public String getMetricProfileName(Integer id){
    
        return this.profileNameIdMap.get(id);
    }
    
}
