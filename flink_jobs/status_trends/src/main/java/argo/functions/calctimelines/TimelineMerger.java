/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.calctimelines;

import argo.pojos.Timeline;

import argo.profiles.OperationsParser;
import argo.utils.EnumStatus;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author cthermolia TimelineMerger , builds the timelines in each step of
 * calculations
 */
public class TimelineMerger {

    private final OperationsParser operationsParser;
    private final String operation;
   
    public TimelineMerger(String operation, OperationsParser operationsParser) {
        this.operationsParser = operationsParser;
        this.operation = operation;
    }

    /**
     *
     * @param timelineList , a list of timeline data
     * @return , a timeline that is created by a list of timelines
     * @throws ParseException
     */
    public Timeline mergeTimelines(ArrayList<Timeline> timelineList) throws ParseException {

        ArrayList<Date> timestamps = collectTimestamps(timelineList);

        TreeMap<Date, ArrayList<String>> statusMap = gatherStatusesPerTimestamp(timestamps, timelineList);

        TreeMap<Date, String> resultMap = operateStatusTimeline(statusMap);
        Timeline mergedTimeline = new Timeline(resultMap);
        return mergedTimeline;
    }

    /**
     *
     * @param timelineList a list of timeline data
     * @return a list of dates descibing all timestamps that can be found in the
     * in list
     */
    public ArrayList<Date> collectTimestamps(ArrayList<Timeline> timelineList) {

        ArrayList<Date> list = new ArrayList<>();
        for (Timeline timeline : timelineList) {
            for (Date dt : timeline.getTimelineMap().keySet()) {
                if (!list.contains(dt)) {
                    list.add(dt);
                }
            }
        }

        TreeSet<Date> collTimelinesSet = new TreeSet<>(list);
      

        ArrayList<Date> timestamps = new ArrayList<>(collTimelinesSet);

        return timestamps;
    }

    /**
     * for each timestamp in the overall timeline , create a status list
     * containing all the statuses of each metric timeline that corresponds to
     * the timestamp of the overall timeline
     *
     * @param timestamps, a list of timestamps describing the timestamps in the
     * timelines
     * @param timelines, a lust of timelines
     * @return , a map where key is timestamps containe in timestamps and for
     * value for each key is a list of statuses , collected from the timelines
     * and correspond to each timestamp
     */
    public TreeMap<Date, ArrayList<String>> gatherStatusesPerTimestamp(ArrayList<Date> timestamps, ArrayList<Timeline> timelines) {
        TreeMap<Date, ArrayList<String>> statusTimestampMap = new TreeMap<>();

        for (Date time : timestamps) { //for each timestamp  T in the overall timeline
            for (Timeline t : timelines) { // for each metric timeline

                TreeMap<Date, String> metricTimeline = t.getTimelineMap();
                if (metricTimeline.containsKey(time)) { // if timestamp T is in the metric timeline
                    ArrayList<String> statusList = statusTimestampMap.get(time);

                    if (statusList == null) {
                        statusList = new ArrayList<>();
                    }
                    String status = metricTimeline.get(time); //get metric timeline status of T
                    statusList.add(status); //add status in overall timeline bucket  <T,{status1, ....} >
                    statusTimestampMap.put(time, statusList);

                } else { // if timestamp T is not in the metric timeline, parse each timestamp in metric timeline and add in overall timeline bucket the status of the timestamp that is previous of the timestamp> T
                    String status = EnumStatus.MISSING.name();
                    Iterator iter = metricTimeline.keySet().iterator();
                    while (iter.hasNext()) {
                        Date tempD = (Date) iter.next();
                        if (tempD.after(time)) {
                            break;
                        }
                        status = metricTimeline.get(tempD);
                    }//for (Date dt : metricTimeline.keySet()) { //for each timestamp in metric timeline

                    ArrayList<String> statusList = statusTimestampMap.get(time);

                    if (statusList == null) {
                        statusList = new ArrayList<>();
                    }
                    statusList.add(status);
                    statusTimestampMap.put(time, statusList);

                }
            }

        }
        return statusTimestampMap;
    }

    /**
     *
     * @param timelineStatusMap, a map that contains for each existing timestamp
     * a list of statuses that correspond to the timestamp
     * @param opTruthTable , a map containing for each operation the
     * corresponding truth table
     * @return , a map containing for each timestamp the status that prevails
     * @throws ParseException
     */
    private TreeMap<Date, String> operateStatusTimeline(TreeMap<Date, ArrayList<String>> timelineStatusMap) throws ParseException {

        TreeMap<Date, String> result = new TreeMap<Date, String>();

        for (Date dt : timelineStatusMap.keySet()) {
            ArrayList<String> statusList = timelineStatusMap.get(dt);

            String finalStatus = null;
            int pos = 0;

            Iterator<String> iter = statusList.iterator();
            while (iter.hasNext()) {
                if (pos == 0) {
                    finalStatus = iter.next();
                } else {
                    String status = iter.next();
                    finalStatus = operationsParser.getStatusFromTruthTable(operation, status, finalStatus);
                }
                pos++;
            }
            result.put(dt, finalStatus);
        }
        return result;
    }

}
