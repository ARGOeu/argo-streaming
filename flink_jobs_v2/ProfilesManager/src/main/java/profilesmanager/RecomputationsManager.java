package profilesmanager;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * RecomputationsManager, provide information about the endpoints for which
 * exist requests about recomputing the generated results for a time period
 */
public class RecomputationsManager {

    private static final Logger LOG = Logger.getLogger(RecomputationsManager.class.getName());

    public Map<String, ArrayList<Map<String, String>>> groups;
    // Recomputations for filtering monitoring engine results
    public Map<String, ArrayList<Map<String, Date>>> monEngines;
    public ArrayList<ExcludedMetric> excludedMetrics;// a list of excluded metrics

    public HashMap<ElementType, List<ChangedStatusItem>> changedStatusItems;

    public RecomputationsManager() {
        this.groups = new HashMap<String, ArrayList<Map<String, String>>>();
        this.monEngines = new HashMap<String, ArrayList<Map<String, Date>>>();
        this.excludedMetrics = new ArrayList<>();
        this.changedStatusItems = new HashMap<>();
    }

    // Clear all the recomputation data
    public void clear() {
        this.groups = new HashMap<String, ArrayList<Map<String, String>>>();
        this.monEngines = new HashMap<String, ArrayList<Map<String, Date>>>();
        this.excludedMetrics = new ArrayList<>();
        this.changedStatusItems = new HashMap<>();
    }

    // Insert new recomputation data for a specific endpoint group
    public void insert(String group, String start, String end) {

        Map<String, String> temp = new HashMap<String, String>();
        temp.put("start", start);
        temp.put("end", end);

        if (this.groups.containsKey(group) == false) {
            this.groups.put(group, new ArrayList<Map<String, String>>());
        }

        this.groups.get(group).add(temp);

    }

    // Insert new recomputation data for a specific monitoring engine
    public void insertMon(String monHost, String start, String end) throws ParseException {

        Map<String, Date> temp = new HashMap<String, Date>();
        SimpleDateFormat tsW3C = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        temp.put("s", tsW3C.parse(start));
        temp.put("e", tsW3C.parse(end));

        if (this.monEngines.containsKey(monHost) == false) {
            this.monEngines.put(monHost, new ArrayList<Map<String, Date>>());
        }

        this.monEngines.get(monHost).add(temp);

    }
    // Insert new recomputation data for a specific endpoint group

    public void insertExcludedMetrics(String group, String service, String hostname, String metric, String start, String end) {

        ExcludedMetric excludedMetric = new ExcludedMetric(group, service, hostname, metric, start, end);
        this.excludedMetrics.add(excludedMetric);

    }
    // Check if metric is excluded in recomputations

    public ExcludedMetric findMetricExcluded(String group, String service, String hostname, String metric) {
        boolean isExcluded = false;
        for (ExcludedMetric excludedMetric : this.excludedMetrics) {
            if (excludedMetric.metric == null || excludedMetric.metric.equals(metric)) {
                isExcluded = isEqualGroup(excludedMetric, group, service, hostname);
                if (isExcluded) {
                    return excludedMetric;
                }
            } else if (excludedMetric.metric.equals(metric)) {
                return excludedMetric;
            }
        }
        return null;
    }

    public ChangedStatusItem findChangedStatusItem(String group, String service, String hostname, String metric, ElementType elementType) {
        boolean isExcluded = false;
        List<ChangedStatusItem> changedStatusItemsList = this.changedStatusItems.get(elementType);
        if (changedStatusItemsList != null) {
            for (ChangedStatusItem item : changedStatusItemsList) {
                if (elementType.equals(ElementType.GROUP) && item.getGroup().equals(group)) {
                    return item;
                }
                if (elementType.equals(ElementType.SERVICE) && item.getService().equals(group)) {
                    return item;
                }
                if (elementType.equals(ElementType.ENDPOINT) && item.getHostname().equals(group)) {
                    return item;
                }
                if (elementType.equals(ElementType.METRIC) && item.getMetric().equals(group)) {
                    boolean metricMatches = item.getMetric().equals(metric);
                    if (metricMatches && isEqualGroup(item, group, service, hostname)) {
                        return item;
                    }
                }
            }
            return null;
        }
        return null;
    }

    private boolean isEqualGroup(ExcludedMetric excludedMetric, String group, String service, String hostname) {
        if (excludedMetric.getGroup() == null || excludedMetric.getGroup().equals(group)) {
            return isEqualService(excludedMetric, service, hostname);
        }
        return false;
    }

    private boolean isEqualService(ExcludedMetric excludedMetric, String service, String hostname) {
        if (excludedMetric.getService() == null || excludedMetric.getService().equals(service)) {
            return isEqualHostname(excludedMetric, hostname);
        }
        return false;
    }

    private boolean isEqualHostname(ExcludedMetric excludedMetric, String hostname) {
        if (excludedMetric.getHostname() == null || excludedMetric.getHostname().equals(hostname)) {
            return true;
        }
        return false;

    }

    // Check if group is excluded in recomputations
    public boolean isExcluded(String group) {
        return this.groups.containsKey(group);
    }

    // Check if a recomputation period is valid for target date
    public boolean validPeriod(String target, String start, String end) throws ParseException {

        SimpleDateFormat dmy = new SimpleDateFormat("yyyy-MM-dd");
        Date tDate = dmy.parse(target);
        Date sDate = dmy.parse(start);
        Date eDate = dmy.parse(end);

        return (tDate.compareTo(sDate) >= 0 && tDate.compareTo(eDate) <= 0);

    }

    public ArrayList<Map<String, String>> getPeriods(String group, String targetDate) throws ParseException {
        ArrayList<Map<String, String>> periods = new ArrayList<Map<String, String>>();

        if (this.groups.containsKey(group)) {
            for (Map<String, String> period : this.groups.get(group)) {
                if (this.validPeriod(targetDate, period.get("start"), period.get("end"))) {
                    periods.add(period);
                }
            }

        }

        return periods;
    }

    // 
    public boolean isMonExcluded(String monHost, String inputTs) throws ParseException {

        if (this.monEngines.containsKey(monHost) == false) {
            return false;
        }
        SimpleDateFormat tsW3C = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date targetDate = tsW3C.parse(inputTs);
        for (Map<String, Date> item : this.monEngines.get(monHost)) {

            if (!(targetDate.before(item.get("s")) || targetDate.after(item.get("e")))) {
                return true;
            }
        }

        return false;
    }

    private void readJson(JsonElement jRootElement) throws ParseException {
        try {
            JsonArray jRootObj = jRootElement.getAsJsonArray();

            for (JsonElement item : jRootObj) {

                // Get the excluded sites 
                if (item.getAsJsonObject().get("start_time") != null
                        && item.getAsJsonObject().get("end_time") != null
                        && item.getAsJsonObject().get("exclude") != null) {

                    String start = item.getAsJsonObject().get("start_time").getAsString();
                    String end = item.getAsJsonObject().get("end_time").getAsString();

                    // Get the excluded
                    JsonArray jExclude = item.getAsJsonObject().get("exclude").getAsJsonArray();
                    for (JsonElement subitem : jExclude) {
                        this.insert(subitem.getAsString(), start, end);
                    }
                    JsonArray jExcludeMetrics = new JsonArray();
                    if (item.getAsJsonObject().has("exclude_metrics")) {
                        jExcludeMetrics = item.getAsJsonObject().get("exclude_metrics").getAsJsonArray();
                    }
                    for (JsonElement subitemMetric : jExcludeMetrics) {
                        String hostname = null;
                        String service = null;
                        String group = null;
                        String metric = null;
                        if (subitemMetric.getAsJsonObject().get("hostname") != null) {
                            hostname = subitemMetric.getAsJsonObject().get("hostname").getAsString();
                        }
                        if (subitemMetric.getAsJsonObject().get("service") != null) {
                            service = subitemMetric.getAsJsonObject().get("service").getAsString();
                        }
                        if (subitemMetric.getAsJsonObject().get("group") != null) {
                            group = subitemMetric.getAsJsonObject().get("group").getAsString();
                        }
                        if (subitemMetric.getAsJsonObject().get("metric") != null) {
                            metric = subitemMetric.getAsJsonObject().get("metric").getAsString();
                        }
                        this.insertExcludedMetrics(group, service, hostname, metric, start, end);
                    }

                }
                // Get the excluded Monitoring sources
                if (item.getAsJsonObject().get("exclude_monitoring_source") != null) {
                    JsonArray jMon = item.getAsJsonObject().get("exclude_monitoring_source").getAsJsonArray();
                    for (JsonElement subitem : jMon) {

                        String monHost = subitem.getAsJsonObject().get("host").getAsString();
                        String monStart = subitem.getAsJsonObject().get("start_time").getAsString();
                        String monEnd = subitem.getAsJsonObject().get("end_time").getAsString();
                        this.insertMon(monHost, monStart, monEnd);
                    }
                }
                // Get the excluded Monitoring sources
                if (item.getAsJsonObject().get("applied_status_changes") != null) {
                    JsonArray jStatus = item.getAsJsonObject().get("applied_status_changes").getAsJsonArray();
                    String start = item.getAsJsonObject().get("start_time").getAsString();
                    String end = item.getAsJsonObject().get("end_time").getAsString();

                    for (JsonElement element : jStatus) {
                        JsonObject itemObj = element.getAsJsonObject();
                        ChangedStatusItem changedItem = new ChangedStatusItem();
                        ElementType elementType = null;
                        String status=null;
                        if (itemObj.has("state")) {
                            status=itemObj.get("state").getAsString();
                        }
                            // Set all fields
                        if (itemObj.has("group")) {
                            changedItem.setGroup(itemObj.get("group").getAsString());
                        }
                        if (itemObj.has("service")) {
                            changedItem.setService(itemObj.get("service").getAsString());
                        }
                        if (itemObj.has("hostname")) {
                            changedItem.setHostname(itemObj.get("hostname").getAsString());
                        }
                        if (itemObj.has("metric") || itemObj.has("netric")) {
                            String metric = itemObj.has("metric") ? itemObj.get("metric").getAsString() : itemObj.get("netric").getAsString();
                            changedItem.setMetric(metric);
                        }
                        changedItem.setStartPeriod(start);
                        changedItem.setEndPeriod(end);
                        changedItem.setStatus(status);

                        // Priority logic
                        if (changedItem.getMetric() != null) {
                            elementType = ElementType.METRIC;
                        } else if (changedItem.getHostname() != null) {
                            elementType = ElementType.ENDPOINT;
                        } else if (changedItem.getService() != null) {
                            elementType = ElementType.SERVICE;
                        } else if (changedItem.getGroup() != null) {
                            elementType = ElementType.GROUP;
                        }
                        if (elementType != null) {

                            changedItem.setElementType(elementType);
                            List<ChangedStatusItem> list = changedStatusItems.get(elementType);
                            if (list == null) {
                                list = new ArrayList<>();
                                changedStatusItems.put(elementType, list);
                            }
                            list.add(changedItem);
                        }
                    }
//                        for (JsonElement subitem : jStatus) {
//                        String status = subitem.getAsJsonObject().get("state").getAsString();
//
//
//                        JsonArray statusGroups = subitem.getAsJsonObject().get("groups").getAsJsonArray();
//                        for (JsonElement statusItem : statusGroups) {
//                            System.out.println("status item is : " + statusItem.getAsString());
//                            String statusGroup = statusItem.getAsString();
//                            this.insertChangedStatus(statusGroup, status, start, end, ElementType.GROUP);
//                        }
//                    }
                }

            }
        } catch (ParseException pex) {
            LOG.error("Parsing date error");
            throw pex;
        }

    }

    public void loadJson(File jsonFile) throws IOException, ParseException {

        this.clear();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser jsonParser = new JsonParser();
            JsonElement jRootElement = jsonParser.parse(br);
            readJson(jRootElement);

        } catch (FileNotFoundException ex) {
            LOG.error("Could not open file:" + jsonFile.getName());
            throw ex;

        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(br);
        }

    }

    /**
     * Load Recompuatation information from a JSON string instead of a File
     * source. This method is used in execution enviroments where the required
     * data is provided by broadcast variables
     */
    public void loadJsonString(List<String> recJson) throws IOException, ParseException {

        this.clear();

        try {

            JsonParser jsonParser = new JsonParser();
            JsonElement jRootElement = jsonParser.parse(recJson.get(0));
            if (jRootElement.isJsonNull()) {
                return;
            }
            readJson(jRootElement);

        } catch (ParseException pex) {
            LOG.error("Parsing date error");
            throw pex;

        }
    }

    public class ExcludedMetric {

        private String group;
        private String service;
        private String hostname;
        private String metric;
        private String startPeriod;
        private String endPeriod;

        public ExcludedMetric() {
        }

        public ExcludedMetric(String group, String service, String hostname, String metric, String startPeriod, String endPeriod) {
            this.group = group;
            this.service = service;
            this.hostname = hostname;
            this.metric = metric;
            this.startPeriod = startPeriod;
            this.endPeriod = endPeriod;
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

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public String getStartPeriod() {
            return startPeriod;
        }

        public void setStartPeriod(String startPeriod) {
            this.startPeriod = startPeriod;
        }

        public String getEndPeriod() {
            return endPeriod;
        }

        public void setEndPeriod(String endPeriod) {
            this.endPeriod = endPeriod;
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
            final ExcludedMetric other = (ExcludedMetric) obj;
            if (!Objects.equals(this.group, other.group)) {
                return false;
            }
            if (!Objects.equals(this.service, other.service)) {
                return false;
            }
            if (!Objects.equals(this.hostname, other.hostname)) {
                return false;
            }
            if (!Objects.equals(this.metric, other.metric)) {
                return false;
            }
            if (!Objects.equals(this.startPeriod, other.startPeriod)) {
                return false;
            }
            if (!Objects.equals(this.endPeriod, other.endPeriod)) {
                return false;
            }
            return true;
        }

    }


    public class ChangedStatusItem extends ExcludedMetric {

        private String status;
        private ElementType elementType;

        public ChangedStatusItem() {
        }

        public ChangedStatusItem(String group, String service, String hostname, String metric, String startPeriod, String endPeriod, String status, ElementType type) {
            super(group, service, hostname, metric, startPeriod, endPeriod);
            this.status = status;
            this.elementType = type;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public ElementType getElementType() {
            return elementType;
        }

        public void setElementType(ElementType elementType) {
            this.elementType = elementType;
        }
    }

    public enum ElementType {
        GROUP,
        SERVICE,
        ENDPOINT,
        METRIC
    }
}
