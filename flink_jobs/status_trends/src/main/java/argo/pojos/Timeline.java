/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

import argo.utils.Utils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author cthermolia Timeline is a wrapper on the timeline tree map, adding
 * functionality to the timeline
 */
public class Timeline {

    private TreeMap<Date, String> timelineMap; // a map of <Date, Status> pairs that depicts the timeline info
    private String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public Timeline() {
    }

    public Timeline(TreeMap<Date, String> timelineMap) {
        this.timelineMap = timelineMap;
    }

    public TreeMap<Date, String> getTimelineMap() {
        return timelineMap;
    }

    public void setTimelineMap(TreeMap<Date, String> timelineMap) {
        this.timelineMap = timelineMap;
    }

    public void addTimestamp(Date date, String status) {
        if (timelineMap == null) {
            timelineMap = new TreeMap<>();
        }
        timelineMap.put(date, status);
    }

    public void addTimestamp(String date, String status) throws ParseException {
        if (timelineMap == null) {
            timelineMap = new TreeMap<>();
        }
        timelineMap.put(Utils.convertStringtoDate(format,date), status);
    }

    public String retrieveStatus(String dateStr) throws ParseException {
        if (timelineMap == null) {
            return null;
        }
        Date date = Utils.convertStringtoDate(format, dateStr);
        String status = timelineMap.get(date);
        if (status == null) {
            Iterator iter = timelineMap.keySet().iterator();
            while (iter.hasNext()) {
                Date tempD = (Date) iter.next();
                if (date.before(tempD)) {
                    break;
                }
                status = timelineMap.get(tempD);
            }
        }
        return status;
    }

    /*  
     * @return , a list of dates descibing all timestamps that can be found in the timeline
     */
    public ArrayList<String> collectTimestampsInTimelineStringFormat() throws ParseException {
        //ArrayList<TimelineTrends> timelist = new ArrayList<>();
        if (timelineMap == null) {
            return null;
        }
        ArrayList<String> timeline = new ArrayList<>();

        for (Date time : timelineMap.keySet()) {
            timeline.add(Utils.convertDateToString(format, time));
        }
        return timeline;
    }

    /*  
     * @return , a list of dates descibing all timestamps that can be found in the timeline
     */
    public ArrayList<Date> collectTimestampsInTimeline() {

        if (timelineMap == null) {
            return null;
        }
        ArrayList<Date> timeline = new ArrayList<>();

        for (Date time : timelineMap.keySet()) {
            timeline.add(time);
        }
        return timeline;
    }

    /**
     *
     * @return a number of status changes that occur on the timeline
     */
    public Integer calculateStatusChanges() {
        if (timelineMap == null) {
            return null;
        }
        String previousStatus = null;
        int flipflop = 0;
        for (Map.Entry<Date, String> entry : timelineMap.entrySet()) {
            String status = entry.getValue();
            if (previousStatus != null && status != null && !status.equalsIgnoreCase(previousStatus)) {
                flipflop++;
            }
            previousStatus = status;
        }
        return flipflop;
    }

    // add to map an entry of (T00:00:00, status) and an entry of (T23:59:59, status) 
    public void manageFirstLastTimestamps() throws ParseException {
        if (timelineMap == null) {
            return;
        }
        Map.Entry<Date, String> firstEntry = timelineMap.firstEntry(); 
        Map.Entry<Date, String> lastEntry = timelineMap.lastEntry(); 
        String status = firstEntry.getValue();
        Date timestamp = Utils.createDate(format, lastEntry.getKey(), 0, 0, 0); // create a timestamp of the date with time 00:00:00
        if (Utils.isPreviousDate(format, timestamp, firstEntry.getKey())) {  //if exists a previous day timestamp get the status of the timestamp, else set the status=MISSING
            status = firstEntry.getValue();
            timelineMap.remove(firstEntry.getKey()); //remove the first entry 
        } else {
            status = "MISSING";
        }

        timelineMap.put(timestamp, status);

        status = lastEntry.getValue(); // get the last timestamp status
        timestamp = Utils.createDate(format, lastEntry.getKey(), 23, 59, 59);//create timestamp on 23:59:59 time
        timelineMap.put(timestamp, status); // add or update (with same status) the map

    }

//    public String getFormat() {
//        return format;
//    }
//
//    public void setFormat(String format) {
//        this.format = format;
//    }
}
