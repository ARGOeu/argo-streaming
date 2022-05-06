package argo.batch;

import argo.ar.EndpointAR;
import argo.ar.EndpointGroupAR;
import argo.ar.ServiceAR;
import argo.avro.GroupGroup;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import static org.apache.commons.math3.util.Precision.round;
import org.apache.flink.api.java.tuple.Tuple8;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.AggregationProfileManager;
import profilesmanager.GroupGroupManager;
import profilesmanager.OperationsManager;
import timelines.Timeline;
import trends.calculations.Trends;
import utils.Utils;

/**
 *
 * Utils class to prepare datasets
 */
public class TestUtils {

    static Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static enum LEVEL { // an enum class to define the level of the calculations
        GROUP, SERVICE, FUNCTION, HOSTNAME, METRIC

    }

    /**
     * Prepares the expected dataset for each level of status timelines, by
     * grouping the same timelines for each level and merging their timestatuses
     * , concluding into a new timeline
     *
     * @param list
     * @param opsDS
     * @param aggrDS
     * @param level
     * @return
     * @throws ParseException
     * @throws IOException
     */
    public static ArrayList<StatusTimeline> prepareLevelTimeline(List<StatusTimeline> list, List<String> opsDS, List<String> aggrDS, LEVEL level) throws ParseException, IOException {

        String group = "", function = "", service = "", hostname = "", metric = "";

        ArrayList<ArrayList<StatusTimeline>> alltimelines = new ArrayList<>();
        ArrayList<StatusTimeline> tl = new ArrayList<>();
        boolean added = false;
        int i = 0;

        for (StatusTimeline sm : list) {

            if (i == 0) {
                group = sm.getGroup();

                service = sm.getService();
                hostname = sm.getHostname();
                function = sm.getFunction();
                initEmptyProperties(sm, level);
                tl.add(sm);
                i++;
                continue;
            }

            if (level.equals(LEVEL.HOSTNAME) && !(sm.getGroup().equals(group) && sm.getFunction().equals(function) && sm.getService().equals(service) && sm.getHostname().equals(hostname))) {
                i++;
                //   storeAndProceed(alltimelines, tl, sm);
                alltimelines.add(tl);
                //    }
                tl = new ArrayList<>();
                tl.add(sm);
                added = false;
                group = sm.getGroup();
                service = sm.getService();
                hostname = sm.getHostname();
                function = sm.getFunction();

                continue;
            }
            if (level.equals(LEVEL.SERVICE) && !(sm.getGroup().equals(group) && sm.getFunction().equals(function) && sm.getService().equals(service))) {
                i++;
                // storeAndProceed(alltimelines, tl, sm);
                alltimelines.add(tl);
                //    }
                tl = new ArrayList<>();
                tl.add(sm);
                sm.setHostname("");
                sm.setMetric("");
                added = false;
                group = sm.getGroup();
                service = sm.getService();
                function = sm.getFunction();

                continue;
            }
            if (level.equals(LEVEL.FUNCTION) && !(sm.getGroup().equals(group) && sm.getFunction().equals(function))) {
                i++;
                // storeAndProceed(alltimelines, tl, sm);
                alltimelines.add(tl);
                //    }
                tl = new ArrayList<>();
                tl.add(sm);
                sm.setHostname("");
                sm.setMetric("");
                sm.setService("");

                added = false;
                group = sm.getGroup();
                function = sm.getFunction();
                continue;
            }
            if (level.equals(LEVEL.GROUP) && !(sm.getGroup().equals(group))) {
                i++;
                alltimelines.add(tl);
                tl = new ArrayList<>();
                tl.add(sm);
                sm.setHostname("");
                sm.setMetric("");
                sm.setService("");
                sm.setFunction("");

                added = false;
                group = sm.getGroup();

                continue;
            }
            group = sm.getGroup();
            service = sm.getService();
            hostname = sm.getHostname();
            function = sm.getFunction();

            initEmptyProperties(sm, level);

            tl.add(sm);

            i++;

        }

        if (!added) {
            alltimelines.add(tl);
        }
        ArrayList<StatusTimeline> timelines = parseEndpTimelines(alltimelines, opsDS, aggrDS, level);

        return timelines;
    }

    private static ArrayList<StatusTimeline> parseEndpTimelines(ArrayList<ArrayList<StatusTimeline>> timelist, List<String> opsDS, List<String> aggrDS, LEVEL level) throws ParseException, IOException {
        OperationsManager opsMgr = new OperationsManager();
        opsMgr.loadJsonString(opsDS);

        AggregationProfileManager aggrMgr = new AggregationProfileManager();
        aggrMgr.loadJsonString(aggrDS);

        int i = 0;
        ArrayList<StatusTimeline> timelines = new ArrayList<>();

        for (ArrayList<StatusTimeline> list : timelist) {
            String group = "", function = "", service = "", hostname = "";
            TreeMap<Long, ArrayList<Integer>> map = new TreeMap();
            ArrayList<Timeline> uniquesTimelines = new ArrayList<>();
            for (StatusTimeline sm : list) {
                group = sm.getGroup();
                service = sm.getService();
                function = sm.getFunction();
                hostname = sm.getHostname();

                Timeline timeline = new Timeline();
                for (TimeStatus ts : sm.getTimestamps()) {
                    timeline.insert(new DateTime(ts.getTimestamp()), ts.getStatus());

                }
                timeline.optimize();
                uniquesTimelines.add(timeline);
            }
            int count = 0;
            Timeline initialTimeline = null;
            for (Timeline timeline : uniquesTimelines) {
                if (count == 0) {
                    initialTimeline = timeline;
                } else {
                    int op = -1;
                    if (level.equals(LEVEL.HOSTNAME)) {
                        op = opsMgr.getIntOperation(aggrMgr.getMetricOpByProfile());
                    }
                    if (level.equals(LEVEL.SERVICE)) {
                        op = opsMgr.getIntOperation(aggrMgr.retrieveServiceOperations().get(service));
                    }
                    if (level.equals(LEVEL.FUNCTION)) {
                        op = opsMgr.getIntOperation(aggrMgr.retrieveGroupOperations().get(function));
                    }
                    if (level.equals(LEVEL.GROUP)) {
                        op = opsMgr.getIntOperation(aggrMgr.retrieveProfileOperation());

                    }
                    initialTimeline.aggregate(timeline, opsMgr.getTruthTable(), op);
                }
                count++;
            }
            ArrayList<TimeStatus> finalTimestamps = new ArrayList();
            for (Map.Entry<DateTime, Integer> entry : initialTimeline.getSamples()) {

                TimeStatus ts = new TimeStatus(entry.getKey().getMillis(), entry.getValue());
                finalTimestamps.add(ts);
            }

            StatusTimeline statusTimeline = new StatusTimeline(group, function, service, hostname, "", finalTimestamps);
            timelines.add(statusTimeline);

        }

        return timelines;
    }

    private static void initEmptyProperties(StatusTimeline sm, LEVEL level) {
        if (level.equals(LEVEL.SERVICE)) {
            sm.setHostname("");
            sm.setMetric("");
        }
        if (level.equals(LEVEL.HOSTNAME)) {

            sm.setMetric("");
        }

        if (level.equals(LEVEL.FUNCTION)) {

            sm.setHostname("");
            sm.setMetric("");
            sm.setService("");
        }
        if (level.equals(LEVEL.GROUP)) {

            sm.setHostname("");
            sm.setMetric("");
            sm.setService("");
            sm.setFunction("");
        }

    }

    public static ArrayList<StatusTimeline> prepareMetricTimeline(List<StatusMetric> list, List<String> opsDS) throws ParseException {
        OperationsManager opsMgr = new OperationsManager();
        opsMgr.loadJsonString(opsDS);

        int i = 0;

        String group = "", function = "", service = "", hostname = "", metric = "";

        ArrayList<ArrayList<StatusMetric>> alltimelines = new ArrayList<>();
        ArrayList<StatusMetric> tl = new ArrayList<StatusMetric>();
        boolean added = false;
        for (StatusMetric sm : list) {

            if (i == 0) {
                group = sm.getGroup();
                service = sm.getService();
                hostname = sm.getHostname();
                function = sm.getFunction();
                metric = sm.getMetric();
                tl.add(sm);
                i++;
                continue;
            }
            if (sm.getGroup().equals(group) && sm.getFunction().equals(function) && sm.getService().equals(service) && sm.getHostname().equals(hostname) && sm.getMetric().equals(metric)) {

                tl.add(sm);

            } else {
                Collections.sort(tl, new ListComparator());
                alltimelines.add(tl);
                added = true;
                tl = new ArrayList<>();
                tl.add(sm);
                added = false;
            }
            group = sm.getGroup();
            service = sm.getService();
            hostname = sm.getHostname();
            function = sm.getFunction();
            metric = sm.getMetric();

            i++;
        }
        if (!added) {
            Collections.sort(tl, new ListComparator());
            alltimelines.add(tl);

        }

        ArrayList<StatusTimeline> timelines = parseTimelines(alltimelines, opsDS);

        return timelines;
    }

    private static ArrayList<StatusTimeline> parseTimelines(ArrayList<ArrayList<StatusMetric>> timelist, List<String> opsDS) throws ParseException {
        OperationsManager opsMgr = new OperationsManager();
        opsMgr.loadJsonString(opsDS);

        int i = 0;
        ArrayList<StatusTimeline> timelines = new ArrayList<>();

        for (ArrayList<StatusMetric> list : timelist) {
            ArrayList<TimeStatus> timestatusList = new ArrayList();
            String group = "", function = "", service = "", hostname = "", metric = "";

            for (StatusMetric sm : list) {
                group = sm.getGroup();
                service = sm.getService();
                hostname = sm.getHostname();
                function = sm.getFunction();
                metric = sm.getMetric();
                String timestamp = sm.getTimestamp();
                String prevState = sm.getPrevState();
                String prevTs = sm.getPrevTs();
                String midnight = "2022-01-14T00:00:00Z";
                long ltimestamp = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", timestamp).getMillis();
                long lprevTs = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", prevTs).getMillis();
                long lmidnight = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", midnight).getMillis();

                if (lprevTs < lmidnight && ltimestamp != lmidnight) {
                    TimeStatus timestatus = new TimeStatus(lmidnight, opsMgr.getIntStatus(prevState));
                    timestatusList.add(timestatus);
                    TimeStatus timestatus1 = new TimeStatus(ltimestamp, opsMgr.getIntStatus(sm.getStatus()));
                    timestatusList.add(timestatus1);
                } else {
                    if (sm.getPrevState().equals("MISSING") && ltimestamp != lmidnight) {
                        TimeStatus timestatus = new TimeStatus(lmidnight, opsMgr.getIntStatus(sm.getPrevState()));
                        timestatusList.add(timestatus);
                    }

                    TimeStatus timestatus = new TimeStatus(ltimestamp, opsMgr.getIntStatus(sm.getStatus()));
                    timestatusList.add(timestatus);
                }
            }

            StatusTimeline statusTimeline = new StatusTimeline(group, function, service, hostname, metric, timestatusList);
            timelines.add(statusTimeline);
        }

        return timelines;
    }

    static class ListComparator implements Comparator<StatusMetric> {

        // override the compare() method
        public int compare(StatusMetric s1, StatusMetric s2) {

            try {
                long t1 = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", s1.getTimestamp()).getMillis();
                long t2 = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", s2.getTimestamp()).getMillis();

                if (t1 == t2) {
                    return 0;
                } else if (t1 > t2) {
                    return 1;
                } else {
                    return -1;
                }
            } catch (ParseException ex) {
                java.util.logging.Logger.getLogger(TestUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
            return 0;
        }
    }
   //compares the elements of two lists to decide if the lists are the same

    public static boolean compareLists(List listA, List listB) {

        if (listA == null && listB == null) {
            return true;
        }
        if (listA == null || listB == null) {
            return false;
        }

        List tempA = new ArrayList(listA);
        List tempB = new ArrayList(listB);

        if (tempA.size() != tempB.size()) {
            return false;
        }
        Iterator iterA = tempA.iterator();

        while (iterA.hasNext()) { //iterates over the objects of the first list and if the object is found at the second list the object is removed from both lists
            Object oA = iterA.next();
            Iterator iterB = tempB.iterator();

            boolean found = false;
            while (iterB.hasNext()) {
                Object oB = iterB.next();
                if (oA.toString().equals(oB.toString())) {
                    iterB.remove(); //if the 2 objects are equal remove the object from the second list
                    found = true;
                    break;

                }
            }
            if (found) { ///if the 2 objects are equal remove the object from the first list
                iterA.remove();
            }else{
                System.out.println("not found");
            
            }

        }
        boolean isEq = false;
        
        
        if (tempA.isEmpty() && tempB.isEmpty()) { //if the two lists are empty , this means that all objects were found on both lists and the lists are equal
            isEq = true;
        }
        return isEq;
    }
    
    /**
     * Parses the Metric profile content retrieved from argo-web-api and
     * provides a list of MetricProfile avro objects to be used in the next
     * steps of the pipeline
     */
    public static StatisticsItem[] getListStatisticData(String content) {
        List<StatisticsItem> results = new ArrayList<StatisticsItem>();

        JsonParser jsonParser = new JsonParser();
        JsonElement jElement = jsonParser.parse(content);
        JsonArray jRoot = jElement.getAsJsonArray();
        for (int i = 0; i < jRoot.size(); i++) {
            JsonObject jItem = jRoot.get(i).getAsJsonObject();

            String group = jItem.get("group").getAsString();
            String metric = "";
            String hostname = "";
            String service = "";
            String tags = "";
            int flipflops = 0;

            if (jItem.has("metric") && !jItem.get("metric").isJsonNull()) {
                metric = jItem.get("metric").getAsString();
            }

            if (jItem.has("hostname") && !jItem.get("hostname").isJsonNull()) {
                hostname = jItem.get("hostname").getAsString();
            }
            if (jItem.has("service") && !jItem.get("service").isJsonNull()) {
                service = jItem.get("service").getAsString();
            }
            if (jItem.has("flipflops") && !jItem.get("flipflops").isJsonNull()) {
                flipflops = jItem.get("flipflops").getAsInt();
            }
            if (jItem.has("tags") && !jItem.get("tags").isJsonNull()) {
                tags = jItem.get("tags").getAsString();
            }
            JsonArray statistics = jItem.getAsJsonArray("statistics");
            HashMap<String, int[]> statisticMap = new HashMap<>();
            for (JsonElement item : statistics) {
                // service name
                JsonObject itemObj = item.getAsJsonObject();
                String status = itemObj.get("status").getAsString();
                int duration = itemObj.get("duration").getAsInt();
                int frequency = itemObj.get("freq").getAsInt();

                statisticMap.put(status, new int[]{frequency, duration});

            }

            StatisticsItem statItem = new StatisticsItem(group, service, hostname, metric, flipflops, tags, statisticMap);
            results.add(statItem);
        }
        StatisticsItem[] rArr = new StatisticsItem[results.size()];
        rArr = results.toArray(rArr);
        return rArr;
    }


    public static class StatisticsItem {

        private String group;
        private String service;
        private String hostname;
        private String metric;
        private int flipflops;
        private String tags;

        HashMap<String, int[]> statistics;

        public StatisticsItem(String group, String service, String hostname, String metric, int flipflops, String tags, HashMap<String, int[]> statistics) {
            this.group = group;
            this.service = service;
            this.hostname = hostname;
            this.metric = metric;
            this.flipflops = flipflops;
            this.tags = tags;
            this.statistics = statistics;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
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

        public int getFlipflops() {
            return flipflops;
        }

        public void setFlipflops(int flipflops) {
            this.flipflops = flipflops;
        }

        public HashMap<String, int[]> getStatistics() {
            return statistics;
        }

        public void setStatistics(HashMap<String, int[]> statistics) {
            this.statistics = statistics;
        }

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

    }

 public static List<Trends> prepareFlipFlops(List<StatisticsItem> statisticList, LEVEL level) {

        ArrayList<Trends> trendsList = new ArrayList<>();
        for (StatisticsItem item : statisticList) {
            if (item.getFlipflops() != 0) {

                Trends trends = null;
                if (level.equals(LEVEL.HOSTNAME)) {
                    trends = new Trends(item.getGroup(), item.getService(), item.getHostname(), item.getFlipflops());

                } else if (level.equals(LEVEL.SERVICE)) {
                    trends = new Trends(item.getGroup(), item.getService(), item.getFlipflops());

                } else if (level.equals(LEVEL.GROUP)) {
                    trends = new Trends(item.getGroup(), item.getFlipflops());

                } else {
                    trends = new Trends(item.getGroup(), item.getService(), item.getHostname(), item.getMetric(), item.getFlipflops(), item.getTags());
                }
                trendsList.add(trends);
            }
        }
        return trendsList;
    }

    public static List<Tuple8< String, String, String, String, String, Integer, Integer, String>> prepareTrends(List<StatisticsItem> statisticList, LEVEL level) {

        ArrayList<Tuple8< String, String, String, String, String, Integer, Integer, String>> trendsList = new ArrayList<>();
        for (StatisticsItem item : statisticList) {
            String group = item.getGroup();
            String service = item.getService();
            String hostname = item.getHostname();
            String metric = item.getMetric();

            if (level.equals(LEVEL.HOSTNAME)) {
                metric = null;
            } else if (level.equals(LEVEL.SERVICE)) {
                metric = null;
                hostname = null;
            } else if (level.equals(LEVEL.GROUP)) {
                metric = null;
                hostname = null;
                service = null;
            }

            HashMap<String, int[]> statistics = item.getStatistics();
            int duration = 0;
            int freq = 0;
            if (statistics.containsKey("CRITICAL")) {
                freq = statistics.get("CRITICAL")[0];
                duration = statistics.get("CRITICAL")[1];
            }
            Tuple8<String, String, String, String, String, Integer, Integer, String> tupleCritical = new Tuple8<  String, String, String, String, String, Integer, Integer, String>(
                    group, service, hostname, metric, "CRITICAL", freq, duration, item.getTags());

            duration = 0;
            freq = 0;
            if (statistics.containsKey("WARNING")) {
                freq = statistics.get("WARNING")[0];
                duration = statistics.get("WARNING")[1];
            }
            Tuple8<String, String, String, String, String, Integer, Integer, String> tupleWarning = new Tuple8<  String, String, String, String, String, Integer, Integer, String>(
                    group, service, hostname, metric, "WARNING", freq, duration, item.getTags());

            duration = 0;
            freq = 0;
            if (statistics.containsKey("UNKNOWN")) {
                freq = statistics.get("UNKNOWN")[0];
                duration = statistics.get("UNKNOWN")[1];
            }
            Tuple8<String, String, String, String, String, Integer, Integer, String> tupleUnknown = new Tuple8<  String, String, String, String, String, Integer, Integer, String>(
                    group, service, hostname, metric, "UNKNOWN", freq, duration, item.getTags());

            trendsList.add(tupleCritical);
            trendsList.add(tupleWarning);
            trendsList.add(tupleUnknown);
        }

        return trendsList;
    }
    
    public static class ArItem {

        private String group;

        private String service;

        private String hostname;

        private double availability;
        private double reliability;
        private double up;
        private double unknown;
        private double down;

        public ArItem(String group, String service, String hostname, double availability, double reliability, double up, double unknown, double down) {
            this.group = group;
            this.service = service;
            this.hostname = hostname;
            this.availability = availability;
            this.reliability = reliability;
            this.up = up;
            this.unknown = unknown;
            this.down = down;
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

        public double getAvailability() {
            return availability;
        }

        public void setAvailability(double availability) {
            this.availability = availability;
        }

        public double getReliability() {
            return reliability;
        }

        public void setReliability(double reliability) {
            this.reliability = reliability;
        }

        public double getUp() {
            return up;
        }

        public void setUp(double up) {
            this.up = up;
        }

        public double getUnknown() {
            return unknown;
        }

        public void setUnknown(double unknown) {
            this.unknown = unknown;
        }

        public double getDown() {
            return down;
        }

        public void setDown(double down) {
            this.down = down;
        }

    }

    public static List<ArItem> prepareAR(List<StatisticsItem> statisticList, LEVEL level) {

        ArrayList<ArItem> arList = new ArrayList<>();
        for (StatisticsItem item : statisticList) {
            String group = item.getGroup();
            String service = item.getService();
            String hostname = item.getHostname();
            String metric = item.getMetric();
            if(group.equals("Group_1") && service.equals("Service_1B") && hostname.equals("Hostname_1")){
                System.out.println("here we are");
            
            }
            //   if(group.equals("Group_1") && service.equals("Service_1B") && hostname.equals("Hostname_1")){
            if (level.equals(LEVEL.HOSTNAME)) {
                metric = null;
            } else if (level.equals(LEVEL.SERVICE)) {
                metric = null;
                hostname = null;
            } else if (level.equals(LEVEL.GROUP)) {
                metric = null;
                hostname = null;
                service = null;
            }

            HashMap<String, int[]> statistics = item.getStatistics();
            int criticalDur = 0;
            int warningDur = 0;
            int okDur = 0;
            int unknownDur = 0;
            int downtimeDur = 0;
            int missingDur = 0;

            if (statistics.containsKey("CRITICAL")) {
                criticalDur = statistics.get("CRITICAL")[1] * 60;
            }

            if (statistics.containsKey("WARNING")) {
                warningDur = statistics.get("WARNING")[1] * 60;
            }
            if (statistics.containsKey("UNKNOWN")) {
                unknownDur = statistics.get("UNKNOWN")[1] * 60;
            }
            if (statistics.containsKey("OK")) {
                okDur = statistics.get("OK")[1] * 60;
            }
            if (statistics.containsKey("DOWNTIME")) {
                downtimeDur = statistics.get("DOWNTIME")[1] * 60;
            }
            if (statistics.containsKey("MISSING")) {
                missingDur = statistics.get("MISSING")[1] * 60;
            }
            int daySeconds = 86400;

            //int[] upstatusInfo = new int[2];
            double upstatus = 0;
            upstatus = okDur + warningDur;
//        upstatusInfo[0] = okstatusInfo[0] + warningstatusInfo[0];
//        upstatusInfo[1] = okstatusInfo[1] + warningstatusInfo[1];

            double knownPeriod = (double) daySeconds - (double) unknownDur - (double) missingDur;
            double knownScheduled = knownPeriod - (double) downtimeDur;
            double minutesAvail = ((double) upstatus / (double) knownPeriod) * 100;
            if (Double.valueOf(minutesAvail).isNaN()) {
                minutesAvail = -1;
            }
            double minutesRel = ((double) upstatus / knownScheduled) * 100;

            if (Double.valueOf(minutesRel).isNaN()) {
                minutesRel = -1;
            }

            double upT = ((double) upstatus) / (double) daySeconds;
            double availability = round(minutesAvail, 5, BigDecimal.ROUND_HALF_UP);
            double reliability = round(minutesRel, 5, BigDecimal.ROUND_HALF_UP);
            double up_f = round(((double) upstatus / (double) daySeconds), 5, BigDecimal.ROUND_HALF_UP);
            double unknown_f = round((((double) unknownDur + (double) missingDur) / (double) daySeconds), 5, BigDecimal.ROUND_HALF_UP);
            double down_f = round(((double) downtimeDur / (double) daySeconds), 5, BigDecimal.ROUND_HALF_UP);

            ArItem aritem = new ArItem(group, service, hostname, availability, reliability, up_f, unknown_f, down_f);
            arList.add(aritem);
        }
        //   }
        return arList;

    }

    public static ArrayList<EndpointAR> prepareEndpointAR(List<ArItem> arList, String date) {
        int dateInt = Integer.parseInt(date.replace("-", ""));
        ArrayList<EndpointAR> endpointList = new ArrayList<>();
        for (ArItem arItem : arList) {

            EndpointAR endpar = new EndpointAR(dateInt, "04edb428-01e6-4286-87f1-050546736f7c", arItem.getHostname(), arItem.getService(), arItem.getGroup(), arItem.getAvailability(), arItem.getReliability(), arItem.getUp(), arItem.getUnknown(), arItem.getDown(), "");
            endpointList.add(endpar);
        }
        return endpointList;
    }

    public static ArrayList<ServiceAR> prepareServiceAR(List<ArItem> arList, String date) {
        int dateInt = Integer.parseInt(date.replace("-", ""));
        ArrayList<ServiceAR> serviceList = new ArrayList<>();
        for (ArItem arItem : arList) {

            ServiceAR endpar = new ServiceAR(dateInt, "04edb428-01e6-4286-87f1-050546736f7c", arItem.getService(), arItem.getGroup(), arItem.getAvailability(), arItem.getReliability(), arItem.getUp(), arItem.getUnknown(), arItem.getDown());
            serviceList.add(endpar);
        }
        return serviceList;
    }

    public static ArrayList<EndpointGroupAR> prepareGroupR(List<ArItem> arList, String date, String ggGroup, List<GroupGroup> listGroups) {
        int dateInt = Integer.parseInt(date.replace("-", ""));
        GroupGroupManager ggpMgr = new GroupGroupManager();
        ggpMgr.loadFromList(listGroups);
        ArrayList<EndpointGroupAR> groupList = new ArrayList<>();
        for (ArItem arItem : arList) {
            String supergroup = ggpMgr.getGroup(ggGroup, arItem.getGroup());

            EndpointGroupAR endpar = new EndpointGroupAR(dateInt, "04edb428-01e6-4286-87f1-050546736f7c", arItem.getGroup(), supergroup, 0, arItem.getAvailability(), arItem.getReliability(), arItem.getUp(), arItem.getUnknown(), arItem.getDown());
            groupList.add(endpar);
        }
        return groupList;
    }

}
