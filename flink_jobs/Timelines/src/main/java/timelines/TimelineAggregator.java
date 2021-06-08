package timelines;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**

* TimelineAggregator class implements an aggregator which is able to receive a list of different timelines
* and conclude into one timeline by aggregating all the timestamps and the statuses
 */

public class TimelineAggregator {

    private Timeline output;
    private Map<String, Timeline> inputs;

    /**
     *
     * @param timestamp a timestamp
     * @throws ParseException Constructs the TimelineAggregator object
     */
    public TimelineAggregator(String timestamp) throws ParseException {
        this.output = new Timeline(timestamp);
        this.inputs = new HashMap<String, Timeline>();
    }

    /**
     * Constructs the TimelineAggregator object
     */
    public TimelineAggregator() {
        this.output = new Timeline();
        this.inputs = new HashMap<String, Timeline>();

    }

    /**
     *
     * @param inputs, a map of timelines Constructs a TimelineAggregator object,
     * containing the timelines
     */
    public TimelineAggregator(Map<String, Timeline> inputs) {
        this.inputs = inputs;
        this.output = new Timeline();
    }

    /**
     * Clears the input timelines and the output timeline
     */
    public void clear() {
        this.output.clear();
        this.inputs.clear();
    }

    /**
     *
     * @param date
     * @return the date given as input with midnight time (00:00:00) in
     * yyyy-MM-dd'T'HH:mm:ss'Z format
     *
     */
    public String tsFromDate(String date) {
        DateTime tmp_date = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
        tmp_date = fmt.parseDateTime(date);
        tmp_date = tmp_date.withTime(0, 0, 0, 0);
        return tmp_date.toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
    }

    /**
     *
     * @param name the owner of the created timeline
     * @param timestamp , a timestamp
     * @param prevState , a status
     *
     * Creates a timeline with the given status set at midnight of the date
     * defined from timestamp and add this timeline to the input timelines
     */
    public void createTimeline(String name, String timestamp, int prevState) {
        Timeline temp = new Timeline(timestamp, prevState);
        this.inputs.put(name, temp);
    }

    /**
     *
     * @param name , the owner of the created timeline
     * @param timestamp, a timestamp
     * @param status , a status for the given timestamp if the owner does not
     * have an existing timeline add a new timeline to the inputs
     *
     */
    public void insert(String name, String timestamp, int status) {
        // Check if timeline exists, if not create it
        if (this.inputs.containsKey(name) == false) {
            Timeline temp = new Timeline(timestamp, status);
            this.inputs.put(name, temp);
            return;
        }

        this.inputs.get(name).insert(timestamp, status);
    }

    /**
     *
     * @param name the owner of the created timeline
     * @param timestamp, a timestamp
     * @param status , the status of the given timestamp if the owner does not
     * have an existing timeline add a new timeline to the inputs the created
     * timeline contains the given status for the midnight (00:00:00) of the
     * timestamp
     */

    public void setFirst(String name, String timestamp, int status) {
        // Check if timeline exists, if not create it
        if (this.inputs.containsKey(name) == false) {
            Timeline temp = new Timeline(timestamp, status);
            this.inputs.put(name, temp);
            return;
        }

        this.inputs.get(name).setFirst(timestamp, status);
    }

    /**
     *
     * @return the date of the output timeline
     */
    public LocalDate getDate() {
        return output.getDate();
    }

    public Set<Entry<DateTime, Integer>> getSamples() {
        return this.output.getSamples();
    }

    public void clearAndSetDate(String timestamp) {
        this.output = new Timeline(timestamp);
        this.inputs.clear();

    }

    /**
     *
     * @param truthTable a truth table containing all possible status
     * combinations for the existing operations
     * @param op , the operation to be applied in order to aggregate the
     * timeline statuses
     *
     * aggregates the input timelines into one combined output including all the
     * timestamp status combinations as produced from the input timelines
     */
    public void aggregate(int[][][] truthTable, int op) {
        if (this.output != null) {
            this.output.clear();
        }

        //Iterate through all available input timelines and aggregate
        for (Timeline item : this.inputs.values()) {
            this.output.aggregate(item, truthTable, op);
        }

    }

    public Timeline getOutput() {
        return output;
    }

    public void setOutput(Timeline output) {
        this.output = output;
    }

    public Map<String, Timeline> getInputs() {
        return inputs;
    }

    public void setInputs(Map<String, Timeline> inputs) {
        this.inputs = inputs;
    }

}
