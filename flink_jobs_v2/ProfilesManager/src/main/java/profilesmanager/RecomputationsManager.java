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
import java.lang.reflect.Array;
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

    public static Map<String, ArrayList<Map<String, String>>> groups;
    // Recomputations for filtering monitoring engine results
    public static Map<String, ArrayList<Map<String, Date>>> monEngines;
    // public ArrayList<RecomputationElement> excludedMetrics;// a list of excluded metrics
    public static HashMap<String, List<RecomputationElement>> metricRecomputationItems;
    public static HashMap<String, List<RecomputationElement>> endpointRecomputationItems;
    public static HashMap<String, List<RecomputationElement>> serviceRecomputationItems;
    public static HashMap<String, List<RecomputationElement>> groupRecomputationItems;

    public RecomputationsManager() {
        groups = new HashMap<String, ArrayList<Map<String, String>>>();
        monEngines = new HashMap<String, ArrayList<Map<String, Date>>>();
        //    this.excludedMetrics = new ArrayList<>();
        metricRecomputationItems = new HashMap<>();
        metricRecomputationItems = new HashMap<>();
        endpointRecomputationItems = new HashMap<>();
        serviceRecomputationItems = new HashMap<>();
        groupRecomputationItems = new HashMap<>();
    }

    // Clear all the recomputation data
    public static void clear() {
        groups = new HashMap<String, ArrayList<Map<String, String>>>();
        monEngines = new HashMap<String, ArrayList<Map<String, Date>>>();
        //  this.excludedMetrics = new ArrayList<>();
        metricRecomputationItems = new HashMap<>();
        endpointRecomputationItems = new HashMap<>();
        serviceRecomputationItems = new HashMap<>();
        groupRecomputationItems = new HashMap<>();
    }

    // Insert new recomputation data for a specific endpoint group
    public static void insert(String group, String start, String end) {

        Map<String, String> temp = new HashMap<String, String>();
        temp.put("start", start);
        temp.put("end", end);

        if (groups.containsKey(group) == false) {
            groups.put(group, new ArrayList<Map<String, String>>());
        }

        groups.get(group).add(temp);

    }

    // Insert new recomputation data for a specific monitoring engine
    public static void insertMon(String monHost, String start, String end) throws ParseException {

        Map<String, Date> temp = new HashMap<String, Date>();
        SimpleDateFormat tsW3C = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        temp.put("s", tsW3C.parse(start));
        temp.put("e", tsW3C.parse(end));

        if (monEngines.containsKey(monHost) == false) {
            monEngines.put(monHost, new ArrayList<Map<String, Date>>());
        }

        monEngines.get(monHost).add(temp);

    }
    // Insert new recomputation data for a specific endpoint group

    public static void insertExcludedMetrics(String group, String service, String hostname, String metric, String start, String end) {

        RecomputationElement excludedMetric = new RecomputationElement(group, service, hostname, metric, start, end, "EXCLUDED", ElementType.METRIC);
        List<RecomputationElement> elementList = new ArrayList<>();
        if (metricRecomputationItems.get(excludedMetric.getMetric()) != null) {
            elementList = metricRecomputationItems.get(excludedMetric.getMetric());
        }
        elementList.add(excludedMetric);
        metricRecomputationItems.put(excludedMetric.getMetric(), elementList);

    }

    // Check if metric is excluded in recomputations
//
    public static RecomputationElement findMetricExcluded(String group, String service, String hostname, String metric) {
        boolean isExcluded = false;
        for (RecomputationElement excludedMetric : metricRecomputationItems.get(metric)) {
            if (excludedMetric.metric.equals(metric)) {
                isExcluded = isEqualGroup(excludedMetric, group, service, hostname, ElementType.METRIC);
                if (isExcluded) {
                    return excludedMetric;
                }
            }

        }
        return null;
    }

    public static ArrayList<RecomputationElement> findChangedStatusItem(String group, String service, String hostname, String metric, ElementType elementType) {
        List<RecomputationElement> changedStatusItemsList = new ArrayList<>();
        switch (elementType) {
            case METRIC:
                changedStatusItemsList = metricRecomputationItems.get(metric);
                break;
            case ENDPOINT:
                changedStatusItemsList = endpointRecomputationItems.get(hostname);
                break;
            case SERVICE:
                changedStatusItemsList = serviceRecomputationItems.get(service);
                break;
            case GROUP:
                changedStatusItemsList = groupRecomputationItems.get(group);
                break;
        }

        ArrayList<RecomputationElement> recomputationElements = new ArrayList<>();
        if (changedStatusItemsList != null) {
            for (RecomputationElement item : changedStatusItemsList) {
                if (elementType.equals(ElementType.GROUP) && item.getGroup().equals(group)) {
                    recomputationElements.add(item);
                }
                if (elementType.equals(ElementType.SERVICE) && item.getService().equals(service)) {
                    boolean serviceMatches = item.getService().equals(service);
                    if (serviceMatches && isEqualGroup(item, group, service, hostname, ElementType.SERVICE)) {
                        recomputationElements.add(item);
                    }

                }
                if (elementType.equals(ElementType.ENDPOINT) && item.getHostname().equals(hostname)) {
                    boolean endpointMatches = item.getHostname().equals(hostname);
                    if (endpointMatches && isEqualGroup(item, group, service, hostname, ElementType.ENDPOINT)) {
                        recomputationElements.add(item);
                    }
                }
                if (elementType.equals(ElementType.METRIC) && item.getMetric().equals(metric)) {
                    boolean metricMatches = item.getMetric().equals(metric);
                    if (metricMatches && isEqualGroup(item, group, service, hostname, ElementType.METRIC)) {
                        recomputationElements.add(item);
                    }
                }
            }
            return recomputationElements;
        }
        return recomputationElements;
    }

    private static boolean isEqualGroup(RecomputationElement excludedMetric, String group, String service, String hostname, ElementType elementType) {
        if (excludedMetric.getGroup() == null || excludedMetric.getGroup().equals(group)) {
            if (!elementType.equals(ElementType.SERVICE)) {
                return isEqualService(excludedMetric, service, hostname, elementType);
            } else {
                return true;
            }
        }
        return false;
    }

    private static boolean isEqualService(RecomputationElement excludedMetric, String service, String hostname, ElementType elementType) {

        if (excludedMetric.getService() == null || excludedMetric.getService().equals(service)) {
            if (!elementType.equals(ElementType.ENDPOINT)) {
                return isEqualHostname(excludedMetric, hostname, elementType);
            } else {
                return true;
            }
        }
        return false;
    }

    private static boolean isEqualHostname(RecomputationElement excludedMetric, String hostname, ElementType elementType) {
        if (excludedMetric.getHostname() == null || excludedMetric.getHostname().equals(hostname)) {
            return true;
        }
        return false;

    }

    // Check if group is excluded in recomputations
    public static boolean isExcluded(String group) {
        return groups.containsKey(group);
    }

    // Check if a recomputation period is valid for target date
    public static boolean validPeriod(String target, String start, String end) throws ParseException {

        SimpleDateFormat dmy = new SimpleDateFormat("yyyy-MM-dd");
        Date tDate = dmy.parse(target);
        Date sDate = dmy.parse(start);
        Date eDate = dmy.parse(end);

        return (tDate.compareTo(sDate) >= 0 && tDate.compareTo(eDate) <= 0);

    }

    public static ArrayList<Map<String, String>> getPeriods(String group, String targetDate) throws ParseException {
        ArrayList<Map<String, String>> periods = new ArrayList<Map<String, String>>();

        if (groups.containsKey(group)) {
            for (Map<String, String> period : groups.get(group)) {
                if (validPeriod(targetDate, period.get("start"), period.get("end"))) {
                    periods.add(period);
                }
            }

        }

        return periods;
    }

    // 
    public static boolean isMonExcluded(String monHost, String inputTs) throws ParseException {

        if (!monEngines.containsKey(monHost)) {
            return false;
        }
        SimpleDateFormat tsW3C = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date targetDate = tsW3C.parse(inputTs);
        for (Map<String, Date> item : monEngines.get(monHost)) {

            if (!(targetDate.before(item.get("s")) || targetDate.after(item.get("e")))) {
                return true;
            }
        }

        return false;
    }

    private static void readJson(JsonElement jRootElement) throws ParseException {
        try {
            JsonArray jRootObj = jRootElement.getAsJsonArray();
            String id = "";

            for (JsonElement item : jRootObj) {

                if (item.getAsJsonObject().has("id")) {
                    id = item.getAsJsonObject().get("id").getAsString();
                }

                // Get the excluded sites 
                if (item.getAsJsonObject().get("start_time") != null
                        && item.getAsJsonObject().get("end_time") != null
                        && item.getAsJsonObject().get("exclude") != null) {

                    String start = item.getAsJsonObject().get("start_time").getAsString();
                    String end = item.getAsJsonObject().get("end_time").getAsString();

                    // Get the excluded
                    JsonArray jExclude = item.getAsJsonObject().get("exclude").getAsJsonArray();
                    for (JsonElement subitem : jExclude) {
                        insert(subitem.getAsString(), start, end);
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
                        insertExcludedMetrics(group, service, hostname, metric, start, end);
                    }

                }
                // Get the excluded Monitoring sources
                if (item.getAsJsonObject().get("exclude_monitoring_source") != null) {
                    JsonArray jMon = item.getAsJsonObject().get("exclude_monitoring_source").getAsJsonArray();
                    for (JsonElement subitem : jMon) {

                        String monHost = subitem.getAsJsonObject().get("host").getAsString();
                        String monStart = subitem.getAsJsonObject().get("start_time").getAsString();
                        String monEnd = subitem.getAsJsonObject().get("end_time").getAsString();
                        insertMon(monHost, monStart, monEnd);
                    }
                }
                // Get the excluded Monitoring sources
                if (item.getAsJsonObject().get("applied_status_changes") != null) {
                    JsonArray jStatus = item.getAsJsonObject().get("applied_status_changes").getAsJsonArray();
                    String start = item.getAsJsonObject().get("start_time").getAsString();
                    String end = item.getAsJsonObject().get("end_time").getAsString();

                    for (JsonElement element : jStatus) {
                        JsonObject itemObj = element.getAsJsonObject();
                        RecomputationElement changedItem = new RecomputationElement();
                        ElementType elementType = null;
                        String status = null;
                        if (itemObj.has("state")) {
                            status = itemObj.get("state").getAsString();
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
                            List<RecomputationElement> list = metricRecomputationItems.get(changedItem.getMetric());
                            if (list == null) {
                                list = new ArrayList<>();
                            }
                            list.add(changedItem);
                            metricRecomputationItems.put(changedItem.getMetric(), list);

                        } else if (changedItem.getHostname() != null) {
                            elementType = ElementType.ENDPOINT;
                            List<RecomputationElement> list = endpointRecomputationItems.get(changedItem.getHostname());
                            if (list == null) {
                                list = new ArrayList<>();
                            }
                            list.add(changedItem);
                            endpointRecomputationItems.put(changedItem.getHostname(), list);

                        } else if (changedItem.getService() != null) {
                            elementType = ElementType.SERVICE;
                            List<RecomputationElement> list = serviceRecomputationItems.get(changedItem.getService());
                            if (list == null) {
                                list = new ArrayList<>();
                            }
                            list.add(changedItem);
                            serviceRecomputationItems.put(changedItem.getService(), list);

                        } else if (changedItem.getGroup() != null) {
                            elementType = ElementType.GROUP;


                            changedItem.setElementType(elementType);
                            List<RecomputationElement> list = groupRecomputationItems.get(changedItem.getGroup());
                            if (list == null) {
                                list = new ArrayList<>();
                            }
                            list.add(changedItem);
                            groupRecomputationItems.put(changedItem.getGroup(), list);
                        }
                    }

                }
             }
            if (metricRecomputationItems != null) {
                sortRecomputationItems(metricRecomputationItems, ElementType.METRIC);
            }
            if (endpointRecomputationItems != null) {
                sortRecomputationItems(endpointRecomputationItems, ElementType.ENDPOINT);
            }
            if (serviceRecomputationItems != null) {
                sortRecomputationItems(serviceRecomputationItems, ElementType.SERVICE);
            }

        } catch (
                ParseException pex) {
            LOG.error("Parsing date error");
            throw pex;
        }

    }

    public static void loadJson(File jsonFile) throws IOException, ParseException {

        clear();

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
    public static void loadJsonString(List<String> recJson) throws IOException, ParseException {

        clear();

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

    /*** Objects of RecomputationElement keeps info about topology elements
     that are defined to apply recomputations**/
    public static class RecomputationElement {

        private String group;
        private String service;
        private String hostname;
        private String metric;
        private String startPeriod;
        private String endPeriod;

        private String status;
        private ElementType elementType;

        public RecomputationElement() {
        }

        public RecomputationElement(String group, String service, String hostname, String metric, String startPeriod, String endPeriod, String status, ElementType type) {
            this.group = group;
            this.service = service;
            this.hostname = hostname;
            this.metric = metric;
            this.startPeriod = startPeriod;
            this.endPeriod = endPeriod;
            this.status = status;
            this.elementType = type;

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


        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RecomputationElement)) return false;
            RecomputationElement that = (RecomputationElement) o;
            return Objects.equals(group, that.group) && Objects.equals(service, that.service) && Objects.equals(hostname, that.hostname) && Objects.equals(metric, that.metric) && Objects.equals(startPeriod, that.startPeriod) && Objects.equals(endPeriod, that.endPeriod) && Objects.equals(status, that.status) && elementType == that.elementType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(group, service, hostname, metric, startPeriod, endPeriod, status, elementType);
        }
    }


    /**
     * Objects of ChangedStatusItem extend RecomputationElement
     * to keep info about topology items to apply recomputations that
     * request status change for a specific period
     */
    public enum ElementType {
        GROUP,
        SERVICE,
        ENDPOINT,
        METRIC,
        NONE
    }

    public static void sortRecomputationItems(HashMap<String,List<RecomputationElement>> items, ElementType elementType) {

        for (String key : items.keySet()) {

            List<RecomputationElement> elements = items.get(key);

            Collections.sort(elements, new Comparator<RecomputationElement>() {
                public int compare(RecomputationElement a, RecomputationElement b) {
                    int priorityA = getPriority(a, elementType);
                    int priorityB = getPriority(b, elementType);
                    return Integer.compare(priorityA, priorityB);
                }
            });
            items.put(key,elements);
        }
    }

    private static int getPriority(RecomputationElement item, ElementType elementType) {
        boolean hasGroup = item.group != null;
        boolean hasService = item.service != null;
        boolean hasHostname = item.hostname != null;

        if (elementType.equals(ElementType.METRIC)) {
            if (hasHostname && hasService && hasGroup) return 4;
            if ((hasHostname && hasService) ||  (hasService && hasGroup) || (hasHostname && hasGroup)) return 3;
            if (hasGroup || hasService) return 2;
            if (hasHostname) return 2;
            return 1; // Only metric
        } else if (elementType.equals(ElementType.ENDPOINT)) {
            // hostname is always set, vary by service + group
            if (hasService && hasGroup) return 3;
            if (hasGroup || hasService) return 2;
            return 1; // Only hostname
        } else if (elementType.equals(ElementType.SERVICE)) {
            // service is always set, vary by group
            if (hasGroup) return 2;
            return 1; // Only service
        }

        // Fallback if none match
        return 0;
    }


}
