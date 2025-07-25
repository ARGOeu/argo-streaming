package utils;

import argo.batch.StatusMetric;
import org.joda.time.DateTime;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;

import java.text.ParseException;
import java.util.*;

import org.joda.time.DateTime;

import java.text.ParseException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utility class for handling recomputation logic on a timeline of status values.
 */
public class RecompTimelineBuilder {

    /**
     * Applies a recomputation status change to a timeline of samples for a specified date.
     *
     * <p>This method updates a time-series map (`samples`) with a new status provided via
     * `recompItem`, but only for the overlapping period between the recomputation window and
     * the specific day denoted by `runDate`. It ensures that the start and end timestamps of
     * the recomputation window exist in the timeline, and updates all status values in that range.</p>
     *
     * @param samples    A TreeMap of timestamps (DateTime) to status integers.
     * @param recompItem An object representing a change in status over a defined time window.
     * @param runDate    The date (in "yyyy-MM-dd" format) for which to apply the recomputation.
     * @param opsMgr     A utility to convert a status string to an integer representation.
     * @return The updated TreeMap of samples with recomputed status values applied.
     * @throws ParseException If any date string cannot be parsed.
     */
    public static TreeMap<DateTime, Integer> calcRecomputations(
            TreeMap<DateTime, Integer> samples,
            RecomputationsManager.RecomputationElement recompItem,
            String runDate,
            OperationsManager opsMgr) throws ParseException {

        // Format used to parse the recomputation period timestamps
        String formatter = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        // Convert the runDate to DateTime and normalize it to the start of the day
        DateTime today = Utils.convertStringtoDate("yyyy-MM-dd", runDate);
        today = today.withTime(0, 0, 0, 0);

        // Define the end of the current day
        DateTime tomorrow = today.plusDays(1);

        // Convert recomputation period start and end strings to DateTime
        DateTime startRecPeriod = Utils.convertStringtoDate(formatter, recompItem.getStartPeriod());
        DateTime endRecPeriod = Utils.convertStringtoDate(formatter, recompItem.getEndPeriod());

        // Clamp the start of the recomputation to not start before today
        if (startRecPeriod.isBefore(today)) {
            startRecPeriod = today;
        }

        // Clamp the end of the recomputation to not go beyond tomorrow
        if (!endRecPeriod.isBefore(tomorrow)) {
            endRecPeriod = tomorrow;
        }

        // If the endRecPeriod timestamp is not already in the timeline and within range,
        // insert it with the last known value before it to preserve continuity
        if (!samples.containsKey(endRecPeriod) && endRecPeriod.isBefore(tomorrow)) {
            Map.Entry<DateTime, Integer> floorEntry = samples.floorEntry(endRecPeriod);
            if (floorEntry != null) {
                samples.put(endRecPeriod, floorEntry.getValue());
            }
        }

        // If the startRecPeriod timestamp is missing, add it with the new recomputed status
        //if (!samples.containsKey(startRecPeriod)) {
        samples.put(startRecPeriod, opsMgr.getIntStatus(recompItem.getStatus()));
//        }else{
//
//        }

        // Update all entries between startRecPeriod (exclusive) and endRecPeriod (exclusive)
        SortedMap<DateTime, Integer> subMap = samples.subMap(startRecPeriod, false, endRecPeriod, false);

        // Collect keys to remove
        List<DateTime> keysToRemove = new ArrayList<>(subMap.keySet());
        for (DateTime key : keysToRemove) {
            samples.remove(key);
        }

        return samples;
    }

    public static TreeMap<DateTime, StatusMetric> calcRecomputationsMetrics(
            TreeMap<DateTime, StatusMetric> samples,
            RecomputationsManager.RecomputationElement recompItem,
            String runDate,
            OperationsManager opsMgr) throws ParseException {

        // Define formatter for parsing timestamps from recomputation item
        String formatter = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        // Normalize runDate to start of the day
        DateTime today = Utils.convertStringtoDate("yyyy-MM-dd", runDate).withTime(0, 0, 0, 0);
        DateTime tomorrow = today.plusDays(1);

        // Parse recomputation period boundaries
        DateTime startRecPeriod = Utils.convertStringtoDate(formatter, recompItem.getStartPeriod());
        DateTime endRecPeriod = Utils.convertStringtoDate(formatter, recompItem.getEndPeriod());

        // Clamp recomputation bounds to today and tomorrow
        if (startRecPeriod.isBefore(today)) startRecPeriod = today;
        if (!endRecPeriod.isBefore(tomorrow)) endRecPeriod = tomorrow;

        // ----------------- Handle End of Recomputations -----------------

        Map.Entry<DateTime, StatusMetric> lowerEntry = samples.lowerEntry(endRecPeriod);
        if (lowerEntry != null) {
            StatusMetric lower = lowerEntry.getValue();
            StatusMetric endRecPeriodItem = null;
            if (!samples.containsKey(endRecPeriod) && endRecPeriod.isBefore(tomorrow)) {
                endRecPeriodItem = new StatusMetric();

                // Use original recomputation end timestamp string (NOT endRecPeriod)
                endRecPeriodItem.setTimestamp(recompItem.getEndPeriod());

                endRecPeriodItem.setGroup(lower.getGroup());
                endRecPeriodItem.setService(lower.getService());
                endRecPeriodItem.setHostname(lower.getHostname());
                endRecPeriodItem.setMetric(lower.getMetric());
                endRecPeriodItem.setStatus(lower.getStatus());
                endRecPeriodItem.setFunction(lower.getFunction());

                // Parse and set dateInt and timeInt
                String[] tsToken = endRecPeriodItem.getTimestamp().split("Z")[0].split("T");
                endRecPeriodItem.setDateInt(Integer.parseInt(tsToken[0].replace("-", "")));
                endRecPeriodItem.setTimeInt(Integer.parseInt(tsToken[1].replace(":", "")));

                samples.put(endRecPeriod, endRecPeriodItem);

                // Update next element's prevTs and prevState, if it exists
                Map.Entry<DateTime, StatusMetric> nextEntry = samples.higherEntry(endRecPeriod);
                if (nextEntry != null) {
                    nextEntry.getValue().setPrevTs(Utils.convertDateToString(formatter, endRecPeriod));
                    nextEntry.getValue().setPrevState(endRecPeriodItem.getStatus());
                }
            } else if (endRecPeriod.isBefore(tomorrow)) {
                endRecPeriodItem = samples.get(endRecPeriod);
            }

            if (endRecPeriodItem != null) { //in the case the end of
                // Determine proper previous timestamp
                DateTime lowerTs = Utils.convertStringtoDate(formatter, lower.getTimestamp());
                if (lowerTs.isBefore(startRecPeriod)) {
                    endRecPeriodItem.setPrevTs(Utils.convertDateToString(formatter, startRecPeriod));
                } else {
                    endRecPeriodItem.setPrevTs(lower.getTimestamp());
                }
                endRecPeriodItem.setPrevState(recompItem.getStatus());
                endRecPeriodItem.setMessage("This is the end of recomputation period");
                endRecPeriodItem.setOgStatus("");
            }
        }


        // ----------------- Handle Start of Recomputations -----------------

        if (!samples.containsKey(startRecPeriod)) {
            Map.Entry<DateTime, StatusMetric> floorEntryStart = samples.floorEntry(startRecPeriod);
            StatusMetric newMetric = new StatusMetric();
            newMetric.setStatus(recompItem.getStatus());
            newMetric.setTimestamp(Utils.convertDateToString(formatter, startRecPeriod));
            newMetric.setMessage("This is the start of recomputation period");

            // Parse and set dateInt and timeInt
            String[] tsToken = newMetric.getTimestamp().split("Z")[0].split("T");
            newMetric.setDateInt(Integer.parseInt(tsToken[0].replace("-", "")));
            newMetric.setTimeInt(Integer.parseInt(tsToken[1].replace(":", "")));

            StatusMetric refMetric;

            if (floorEntryStart == null) {
                refMetric = samples.firstEntry().getValue();

                DateTime refPrevTs = Utils.convertStringtoDate(formatter, refMetric.getPrevTs());
                DateTime origStart = Utils.convertStringtoDate(formatter, recompItem.getStartPeriod());

                if (refPrevTs.isBefore(origStart)) {
                    newMetric.setPrevTs(refMetric.getPrevTs());
                    newMetric.setPrevState(recompItem.getStatus());
                } else {
                    newMetric.setPrevTs(Utils.convertDateToString(formatter, startRecPeriod));
                    newMetric.setPrevState(recompItem.getStatus());
                }

            } else {
                refMetric = floorEntryStart.getValue();
                newMetric.setPrevTs(refMetric.getTimestamp());
                newMetric.setPrevState(refMetric.getStatus());
            }

            newMetric.setGroup(refMetric.getGroup());
            newMetric.setService(refMetric.getService());
            newMetric.setHostname(refMetric.getHostname());
            newMetric.setMetric(refMetric.getMetric());
            newMetric.setFunction(refMetric.getFunction());

            samples.put(startRecPeriod, newMetric);
        } else {
            // If exists, just update status and message
            StatusMetric existing = samples.get(startRecPeriod);
            existing.setStatus(recompItem.getStatus());
            existing.setMessage("This is the start of recomputation period");
            samples.put(startRecPeriod, existing);
        }

        // ----------------- Update Metrics within Recomp Period -----------------

        SortedMap<DateTime, StatusMetric> affectedMetrics = samples.subMap(startRecPeriod, false, endRecPeriod, false);
        for (Map.Entry<DateTime, StatusMetric> entry : affectedMetrics.entrySet()) {
            DateTime currentKey = entry.getKey();
            Map.Entry<DateTime, StatusMetric> floorEntryCurrent = samples.lowerEntry(currentKey);

            StatusMetric updated = entry.getValue().clone();
            updated.setStatus(recompItem.getStatus());

            if (floorEntryCurrent != null) {
                StatusMetric prev = floorEntryCurrent.getValue();
                updated.setPrevTs(prev.getTimestamp());
                updated.setPrevState(prev.getStatus());
            }

            entry.setValue(updated);
        }

        return samples;
    }

}
