package timelines;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeline class implements objects that store a set of status per timestamp.
 * The set of status/timestamp is stored ascending by the timestamp (in a
 * TreeMap). The key of the map is the timestamp in the form of DateTime and the
 * status is expressed as an integer , given as input.
 *
 * A timeline can be constructed empty and then the map could be passed as
 * parameter and stored.
 *
 * Also a timeline could be constructed by giving a timestamp. The timestamp
 * would define the timeline's date.
 *
 * Also a timeline could be constructed by giving a timestamp and a status. The
 * timestamp would define the timeline's date and the status would be stored as
 * the status of the 00:00:00 timestamp .
 *
 *
 * Timeline supports insert of a pair of timestamp, status
 *
 */
public class Timeline {


    private LocalDate date;

    static Logger LOG = LoggerFactory.getLogger(Timeline.class);

    private TreeMap<DateTime, Integer> samples;

    /**
     * Constructs an empty timeline
     */
    public Timeline() {
        this.date = null;
        this.samples = new TreeMap<DateTime, Integer>();
    }

    /**
     *
     * @param timestamp a timestamp Constructs a timeline where the timestamp
     * would define the date of the timeline *
     *
     */

    public Timeline(String timestamp) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        tmp_date.withTime(0, 0, 0, 0);
        this.date = tmp_date.toLocalDate();
        this.samples = new TreeMap<DateTime, Integer>();
    }

    /**
     *
     * @param timestamp a timestamp
     * @param state , the status that pairs the timestamp Constructs a timeline
     * , where the timestamp defines the timeline's date and the state is paired
     * at a timestamp , describing midnight (00:00:00)
     *
     */
    Timeline(String timestamp, int state) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        tmp_date = tmp_date.withTime(0, 0, 0, 0);
        this.date = tmp_date.toLocalDate();
        this.samples = new TreeMap<DateTime, Integer>();
        this.samples.put(tmp_date, state);
    }

    public int get(String timestamp) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        if (this.samples.floorEntry(tmp_date) == null) {
            return -1;
        }
        return this.samples.floorEntry(tmp_date).getValue();
    }

    public int get(DateTime point) {
        if (this.samples.floorEntry(point) == null) {
            return -1;
            //throw new RuntimeException("no item found in timeline, size of timeline:" + this.samples.size() + "," + point.toString());
        }
        return this.samples.floorEntry(point).getValue();
    }

    /**
     *
     * @param timestamp a timestamp
     * @param status the status for the given timestamp
     *
     * inserts a pair of timestamp, status in the map.
     */
    public void insert(String timestamp, int status) {

        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        this.samples.put(tmp_date, status);
    }

    /**
     *
     * @param timestamp, a timestamp in the form of datetime
     * @param status , the status of the given timestamp
     *
     * inserts a pair of timestamp, status in the map
     *
     */
    public void insert(DateTime timestamp, int status) {
        samples.put(timestamp, status);
    }

    public void insertStringTimeStamps(TreeMap<String, Integer> timestamps, boolean optimize) {
        for (String dt : timestamps.keySet()) {
            int status = timestamps.get(dt);
            this.insert(dt, status);
        }
        if (optimize) {
            this.optimize();
        }
    }

    /*
     * @param timestamps a map of timestamp, status to be stored in the
     * timeline. the timestamps are in the form of datetime
     */
    public void insertDateTimeStamps(TreeMap<DateTime, Integer> timestamps, boolean optimize) {
        for (DateTime dt : timestamps.keySet()) {
            int status = timestamps.get(dt);
            this.insert(dt, status);
        }
        if (optimize) {
            this.optimize();
        }
    }

    /**
     *
     * @param timestamp, a timestamp
     * @param state, the status for the given timestamp
     *
     * inserts in the map of pairs (timestamp, status) a new entry where the new
     * timestamp is the midnight (00:00:00) of the date of the given timestamp
     * and the status is the given state
     */
    public void setFirst(String timestamp, int state) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        this.samples = new TreeMap<DateTime, Integer>();
        tmp_date = tmp_date.withTime(0, 0, 0, 0);
        this.samples.put(tmp_date, state);
        this.optimize();
    }

    /**
     * clears the map of timestamps, status
     */
    public void clear() {
        this.samples.clear();
    }

    /**
     *
     * @param samples an entry set of timestamp,status clears the existing map
     * and stores the new entry set to the empty map
     */
    public void bulkInsert(Set<Map.Entry<DateTime, Integer>> samples) {
        this.samples.clear();
        for (Map.Entry<DateTime, Integer> entry : samples) {
            this.samples.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     *
     * @return the entry set of the map of timestamp, status
     */
    public Set<Map.Entry<DateTime, Integer>> getSamples() {
        return samples.entrySet();
    }

    /**
     *
     * @return the date of the timeline
     */
    public LocalDate getDate() {
        return this.date;
    }

    /**
     *
     * @return the number of the timestamps stored in the map
     */
    public int getLength() {
        return this.samples.size();
    }

    /**
     *
     * @return checks if the map of timestamp,state is empty
     */
    public boolean isEmpty() {
        return this.samples.isEmpty();
    }

    /**
     * optimizes the map of timestamp, status if two or continuous timestamps
     * have the same status then in the map the first timestamp , status is
     * stored when the status of the next timestamp is different from the
     * previous timestamp's status then both timestamp, status pairs are stored
     */
    public void optimize() {
        TreeMap<DateTime, Integer> optimal = new TreeMap<DateTime, Integer>();
        int prevstate = -1;
        for (DateTime key : this.samples.keySet()) {
            int value = this.samples.get(key);
            if (prevstate == -1) {

                optimal.put(key, value);
                prevstate = value;

            }
            if (prevstate != value) {
                optimal.put(key, value);
                prevstate = value;
            }
        }

        this.samples = optimal;
    }

    /**
     *
     * @return return the timestamps in the map
     */
    public Set<DateTime> getPoints() {
        return this.samples.keySet();
    }

    /**
     *
     * @param second, the second timeline whose timestamps,status will be
     * aggregated to the existing timeline timestamp, status
     * @param truthTable
     * @param op aggregate a set of timestamp,status pairs that are stored in a
     * timeline with a set of timestamp,status pairs of a different timeline,
     */
    public void aggregate(Timeline second, int[][][] truthTable, int op) {
        if (this.isEmpty()) {
            this.bulkInsert(second.getSamples());
            // Optimize even when we have a single timeline for aggregation
            this.optimize();
            return;
        }

        Timeline result = new Timeline();

        // Slice for first
        for (DateTime point : this.getPoints()) {
            result.insert(point, -1);
        }
        // Slice for second 
        for (DateTime point : second.getPoints()) {
            result.insert(point, -1);
        }

        // Iterate over result and ask
        for (DateTime point : result.getPoints()) {
            int a = this.get(point);
            int b = second.get(point);
            if (a != -1 && b != -1) {
                int x = -1;
                x = truthTable[op][a][b];
                if (x == -1) {
                    x = truthTable[op][b][a];
                }

                result.insert(point, x);
            }
        }

        result.optimize();

        // Engrave the result in this timeline
        this.clear();
        this.bulkInsert(result.getSamples());
    }

    /**
     *
     * @param timestampList, a list of pairs of timestamp, status where status
     * is in the form of string
     * @param states, a list of the existing states
     * @return a sorted map of timestamp, status pairs in an ascending order
     * receives pairs of timestamp , status where status is a string (e.g "OK",
     * "WARNING") and converts the string to an integer based on the position of
     * the status in the existing list of the states. Next this pair is stored
     * in the map
     *
     */
    public TreeMap<String, Integer> buildStringTimeStampMap(ArrayList<String[]> timestampList, ArrayList<String> states) {

        TreeMap<String, Integer> timestampMap = new TreeMap();

        for (String[] timestamp : timestampList) {

            String time = timestamp[0];
            timestampMap.put(time, states.indexOf(timestamp[1]));
        }
        return timestampMap;
    }

    /**
     *
     * @param timestampList, a list of pairs of timestamp, status where status
     * is in the form of string and timestamp is in the form of a datetime
     * @param states, a list of the existing states
     * @return a sorted map of timestamp, status pairs in an ascending order
     * receives pairs of timestamp , status where status is a string (e.g "OK",
     * "WARNING") and converts the string to an integer based on the position of
     * the status in the existing list of the states. Next this pair is stored
     * in the map
     *
     */
    public TreeMap<DateTime, Integer> buildDateTimeStampMap(ArrayList<String[]> timestampList, ArrayList<String> states) {

        TreeMap<DateTime, Integer> timestampMap = new TreeMap();

        for (String[] timestamp : timestampList) {

            DateTime tmp_date = new DateTime();
            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
            tmp_date = fmt.parseDateTime(timestamp[0]);
            int status = states.indexOf(timestamp[1]);
            timestampMap.put(tmp_date, status);
        }
        return timestampMap;

    }

    /**
     *
     * @param timestamp, a timestamp removes a pair of timestamp , status from
     * the map
     */
    public void removeTimeStamp(DateTime timestamp) {

        if (this.samples.containsKey(timestamp)) {
            Iterator iter = this.samples.keySet().iterator();
            while (iter.hasNext()) {
                DateTime tmpTimestamp = (DateTime) iter.next();
                if (tmpTimestamp.equals(timestamp)) {
                    iter.remove();
                    break;
                }
            }
        }

    }

    /**
     *
     * @return the number of the times a status changes between the timestamps
     * of the timeline , after the map is optimized
     */
    public int calcStatusChanges() {
        this.optimize();
        return this.samples.keySet().size() - 1;
    }

    /**
     *
     * @param date, a timestamp
     * @param availStates , the list of the available states
     *
     * checks if in the map the midnight exists and if not it is added with
     * status "MISSING"
     */
    public void replacePreviousDateStatus(DateTime date, ArrayList<String> availStates, boolean optimize) {

        DateTime firsTime = date;
        firsTime = firsTime.withTime(0, 0, 0, 0);
        DateTime firstEntry = this.samples.lowerKey(firsTime);

        if (firstEntry != null && !firstEntry.equals(firsTime)) {
            int previousStatus = this.samples.get(firstEntry);
            this.samples.put(firsTime, previousStatus);
            this.samples.remove(firstEntry);

        } else if (firstEntry == null) {
            this.samples.put(firsTime, availStates.indexOf("MISSING"));
        }
        if (optimize) {
            this.optimize();
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + Objects.hashCode(this.date);
        hash = 83 * hash + Objects.hashCode(this.samples);
        return hash;
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
        final Timeline other = (Timeline) obj;
        if (!Objects.equals(this.date, other.date)) {
            return false;
        }
        if (!Objects.equals(this.samples, other.samples)) {
            return false;
        }
        return true;
    }

    public int opInt(int[][][] truthTable, int op, int a, int b) {
        int result = -1;
        try {
            result = truthTable[op][a][b];
        } catch (IndexOutOfBoundsException ex) {
            // LOG.info(ex);
            result = -1;
        }

        return result;
    }

    /**
     * aggregates a timeline with a timeline of downtime statuses, defined from
     * the downtime data source
     *
     * @param downtimeline the timeline containing the defined downtime
     * timestamps
     * @param downDefaultInt the downtime status
     */
    public void aggregateDownTime(Timeline downtimeline, int downDefaultInt) {
        if (downtimeline.isEmpty()) {
            return;
        }
        if (this.isEmpty()) {
            this.bulkInsert(downtimeline.getSamples());
            // Optimize even when we have a single timeline for aggregation
            this.optimize();
            return;
        }

        Timeline result = new Timeline();

        // Slice for first
        for (DateTime point : this.getPoints()) {
            result.insert(point, -1);
        }
        // Slice for second 
        for (DateTime point : downtimeline.getPoints()) {
            result.insert(point, -1);
        }

        // Iterate over result and ask
        for (DateTime point : result.getPoints()) {
            int a = this.get(point);
            int b = downtimeline.get(point);
            int x = a;

            if (a == downDefaultInt || b == downDefaultInt) {
                x = downDefaultInt;
            }
            result.insert(point, x);

        }

        result.optimize();

        // Engrave the result in this timeline
        this.clear();
        this.bulkInsert(result.getSamples());
    }

    public void createDownTimeline(ArrayList<String> downPeriod, int defaultDownStatus, DateTime runDateDt) throws ParseException {
        DateTime endDay = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", runDateDt.toDate(), 23, 59, 59);

        Timeline downTimeline = null;
        if (downPeriod != null) {

            DateTime startDown = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", downPeriod.get(0));
            DateTime endDown = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", downPeriod.get(1));
            endDown = endDown.plusMinutes(1);
            TreeMap<DateTime, Integer> downSample = new TreeMap();
            downSample.put(startDown, defaultDownStatus);
            if (!endDown.isAfter(endDay.getMillis())) {
                downSample.put(endDown, -1);
            }
            // We have downtime declared
            downTimeline = new Timeline();
            downTimeline.insertDateTimeStamps(downSample, true);
        }
    }
}
