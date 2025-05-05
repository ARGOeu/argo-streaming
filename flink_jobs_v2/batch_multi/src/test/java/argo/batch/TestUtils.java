package argo.batch;

import argo.ar.EndpointAR;
import argo.ar.EndpointGroupAR;
import argo.ar.ServiceAR;
import argo.avro.Downtime;
import argo.avro.GroupGroup;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.java.tuple.Tuple8;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.*;
import timelines.Timeline;
import trends.calculations.Trends;
import utils.Utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Level;

import static org.apache.commons.math3.util.Precision.round;

/**
 * Utils class to prepare datasets
 */
public class TestUtils {

    static Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static enum LEVEL { // an enum class to define the level of the calculations
        GROUP, SERVICE, FUNCTION, HOSTNAME, METRIC

    }



    //compares the elements of two lists to decide if the lists are the same
    public static boolean compareLists(List listA, List listB) {
        if (listA == null && listB == null) return true;
        if (listA == null || listB == null) return false;
        if (listA.size() != listB.size()) return false;

        // Check if lists contain StatusTimeline objects
        if (!listA.isEmpty() && listA.get(0) instanceof StatusTimeline) {
            StatusTimelineComparator statusTimelineComparator = new StatusTimelineComparator();
            Collections.sort(listA, statusTimelineComparator);
            Collections.sort(listB, statusTimelineComparator);
        }

        List tempA = new ArrayList(listA);
        List tempB = new ArrayList(listB);

        Iterator iterA = tempA.iterator();
        while (iterA.hasNext()) {
            Object oA = iterA.next();
            Iterator iterB = tempB.iterator();
            boolean found = false;
            while (iterB.hasNext()) {
                Object oB = iterB.next();
                if (oA.toString().equals(oB.toString())) {
                    iterB.remove();
                    found = true;
                    break;
                }
            }
            if (found) {
                iterA.remove();
            }
        }

        return tempA.isEmpty() && tempB.isEmpty();
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

    public static List<Tuple8<String, String, String, String, String, Integer, Integer, String>> prepareTrends(List<StatisticsItem> statisticList, LEVEL level) {

        ArrayList<Tuple8<String, String, String, String, String, Integer, Integer, String>> trendsList = new ArrayList<>();
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
            Tuple8<String, String, String, String, String, Integer, Integer, String> tupleCritical = new Tuple8<String, String, String, String, String, Integer, Integer, String>(
                    group, service, hostname, metric, "CRITICAL", freq, duration, item.getTags());

            duration = 0;
            freq = 0;
            if (statistics.containsKey("WARNING")) {
                freq = statistics.get("WARNING")[0];
                duration = statistics.get("WARNING")[1];
            }
            Tuple8<String, String, String, String, String, Integer, Integer, String> tupleWarning = new Tuple8<String, String, String, String, String, Integer, Integer, String>(
                    group, service, hostname, metric, "WARNING", freq, duration, item.getTags());

            duration = 0;
            freq = 0;
            if (statistics.containsKey("UNKNOWN")) {
                freq = statistics.get("UNKNOWN")[0];
                duration = statistics.get("UNKNOWN")[1];
            }
            Tuple8<String, String, String, String, String, Integer, Integer, String> tupleUnknown = new Tuple8<String, String, String, String, String, Integer, Integer, String>(
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
            double upstatus = 0;
            upstatus = okDur + warningDur;

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


    private static class StatusTimelineComparator implements Comparator<StatusTimeline> {
        @Override
        public int compare(StatusTimeline o1, StatusTimeline o2) {
            int result;

            result = nullSafeCompare(o1.getGroup(), o2.getGroup());
            if (result != 0) return result;

            result = nullSafeCompare(o1.getService(), o2.getService());
            if (result != 0) return result;

            result = nullSafeCompare(o1.getFunction(), o2.getFunction());
            if (result != 0) return result;

            result = nullSafeCompare(o1.getHostname(), o2.getHostname());
            if (result != 0) return result;

            result = nullSafeCompare(o1.getMetric(), o2.getMetric());
            if (result != 0) return result;

            return 0;
        }

        private int nullSafeCompare(String s1, String s2) {
            if (s1 == null && s2 == null) return 0;
            if (s1 == null) return -1;
            if (s2 == null) return 1;
            return s1.compareTo(s2);
        }
    }
}



