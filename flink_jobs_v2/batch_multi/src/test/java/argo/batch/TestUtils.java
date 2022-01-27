package argo.batch;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.AggregationProfileManager;
import profilesmanager.OperationsManager;
import timelines.Timeline;
import utils.Utils;

/**
 *
 * Utils class to prepare datasets
 */
public class TestUtils {

    static Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static enum LEVEL { // an enum class to define the level of the calculations
        GROUP, SERVICE, FUNCTION, HOSTNAME

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

//        if (tempA.size() != tempB.size()) {
//            return false;
//        }
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
            }

        }
        boolean isEq = false;
        if (tempA.isEmpty() && tempB.isEmpty()) { //if the two lists are empty , this means that all objects were found on both lists and the lists are equal
            isEq = true;
        }
        return isEq;
    }

}
