package profilesmanager;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/* MetricTagsManager class implements objects that store the information parsed
 * from a json object containing metrics and their defined tags. 
 *
 * The MetricTagsManager keeps info of a list of tags for each metric and stores the information 
 *in a map where the key is the metric. 
 */
public class MetricTagsManager implements Serializable {

    private HashMap<String, ArrayList<String>> metricTags;

    private static final Logger LOG = Logger.getLogger(MetricTagsManager.class.getName());

    /**
     * A constructor of a MetricTagsManager 
     */
    public MetricTagsManager() {
        metricTags = new HashMap<String, ArrayList<String>>();
    }

    /**
     * Clears the stored data of a MetricTagsManager object
     */
    public void clear() {
        this.metricTags = new HashMap<String, ArrayList<String>>();

    }

    /**
     * Inserts new tag information (metric, <>tags) to the MetricTagsManager
     */
    public int insert(String metric, String tag) {
        if (this.metricTags.containsKey(metric)) {
            this.metricTags.get(metric).add(tag);
        } else {
            this.metricTags.put(metric, new ArrayList<String>());
            this.metricTags.get(metric).add(tag);
        }

        return 0; // All good
    }

    /**
     * Returns tags information by metric
     */
    public ArrayList<String> getTags(String metric) {
        if (metricTags.containsKey(metric)) {

            return metricTags.get(metric);
        }
        return new ArrayList<>();

    }

    /**
     * loads from a json file that contain the metric tags , and stores the info
     * to the corresponding fields
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
            JsonObject arr = jElement.getAsJsonObject();
            JsonElement data = arr.getAsJsonArray("data");
            readJson(data);
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
     * Loads information from a config json string
     *
     */
    public void loadJsonString(List<String> tagJson) throws JsonParseException {
        // Clear data
        this.clear();

        try {

            JsonParser jsonParser = new JsonParser();
            // Grab the first - and only line of json from ops data
            JsonElement jElement = jsonParser.parse(tagJson.get(0));
            readJson(jElement);

        } catch (JsonParseException ex) {
            LOG.error("Not valid json contents");
            throw ex;
        }

    }

    /**
     * reads from a JsonElement array and stores the necessary information to
     * the ReportTagsManager objects and add them to the list
     *
     * @param jElement , a JsonElement containing the tenant's report data
     * @return
     */
    public void readJson(JsonElement jElement) {
        JsonArray jArr = jElement.getAsJsonArray();

        if (jArr != null) {
            for (JsonElement mtags : jArr) {
                JsonObject metric = mtags.getAsJsonObject();

                String metricname = metric.get("name").getAsString();
                JsonArray jTags = metric.getAsJsonArray("tags");
                if (jTags != null) {
                    for (JsonElement tag : jTags) {
                        String jTag = tag.getAsString();
                        this.insert(metricname, jTag);
                    }
                }

            }
        }
    }

}
