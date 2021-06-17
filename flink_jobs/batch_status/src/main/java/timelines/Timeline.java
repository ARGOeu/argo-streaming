package timelines;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import ops.OpsManager;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Timeline {

    private LocalDate date;

    static Logger LOG = LoggerFactory.getLogger(Timeline.class);

    private TreeMap<DateTime, Integer> samples;

    public Timeline() {
        this.date = null;
        this.samples = new TreeMap<DateTime, Integer>();

    }

    public Timeline(String timestamp) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        tmp_date.withTime(0, 0, 0, 0);
        this.date = tmp_date.toLocalDate();
        this.samples = new TreeMap<DateTime, Integer>();
    }

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
        return this.samples.floorEntry(tmp_date).getValue();
    }

    public int get(DateTime point) {
        if (this.samples.floorEntry(point) == null) {

            throw new RuntimeException("no item found in timeline, size of timeline:" + this.samples.size() + "," + point.toString());
        }
        return this.samples.floorEntry(point).getValue();
    }

    public void insert(String timestamp, int status) {

        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        this.samples.put(tmp_date, status);
    }

    public void insert(DateTime date, int status) {
        samples.put(date, status);

    }

    public void insertStringTimeStamps(TreeMap<String, Integer> timestamps) {
        for (String dt : timestamps.keySet()) {
            int status = timestamps.get(dt);
            this.insert(dt, status);

        }
    }

    public void insertDateTimeStamps(TreeMap<DateTime, Integer> timestamps) {
        for (DateTime dt : timestamps.keySet()) {
            int status = timestamps.get(dt);
            this.insert(dt, status);
        }
    }

    public void setFirst(String timestamp, int state) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        tmp_date = fmt.parseDateTime(timestamp);
        this.samples = new TreeMap<DateTime, Integer>();
        tmp_date = tmp_date.withTime(0, 0, 0, 0);
        this.samples.put(tmp_date, state);
        this.optimize();
    }

    public void clear() {
        this.samples.clear();
    }

    public void bulkInsert(Set<Map.Entry<DateTime, Integer>> samples) {
        this.samples.clear();
        for (Map.Entry<DateTime, Integer> entry : samples) {
            this.samples.put(entry.getKey(), entry.getValue());
        }
    }

    public Set<Map.Entry<DateTime, Integer>> getSamples() {
        return samples.entrySet();
    }

    public LocalDate getDate() {
        return this.date;
    }

    public int getLength() {
        return this.samples.size();
    }

    public boolean isEmpty() {
        return this.samples.isEmpty();
    }

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
//	

    public Set<DateTime> getPoints() {
        return this.samples.keySet();
    }

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
                try {
                    x = truthTable[op][a][b];
                } catch (IndexOutOfBoundsException ex) {
                    // LOG.info(ex);
                    x = -1;
                }
                result.insert(point, x);
            }
        }

        result.optimize();

        // Engrave the result in this timeline
        this.clear();
        this.bulkInsert(result.getSamples());
    }

    public TreeMap<String, Integer> buildStringTimeStampMap(ArrayList<String[]> timestampList, OpsManager op) {

        TreeMap<String, Integer> timestampMap = new TreeMap();

        for (String[] timestamp : timestampList) {

            String time = timestamp[0];
            int status = op.getIntStatus(timestamp[1]);
            timestampMap.put(time, status);
        }
        return timestampMap;

    }

    public TreeMap<DateTime, Integer> buildDateTimeStampMap(ArrayList<String[]> timestampList, OpsManager op) {

        TreeMap<DateTime, Integer> timestampMap = new TreeMap();

        for (String[] timestamp : timestampList) {

            DateTime tmp_date = new DateTime();
            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
            tmp_date = fmt.parseDateTime(timestamp[0]);
            int status = op.getIntStatus(timestamp[1]);
            timestampMap.put(tmp_date, status);
        }
        return timestampMap;

    }

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

    public int calcStatusChanges() {

        return this.samples.size() - 1;
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

}
