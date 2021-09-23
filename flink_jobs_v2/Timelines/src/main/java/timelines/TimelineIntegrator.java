
package timelines;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
/**

* TimelineIntegrator class implements an integrator which is able to apply calculations on timelines
regarding status duration and a/r results
 */

public class TimelineIntegrator {
    public double availability=0;
    public double reliability=0;

    public double up_f=0;
    public double unknown_f=0;
    public double down_f=0;

    public static double round(double input, int prec, int mode) {
        try {
            BigDecimal inputBD = BigDecimal.valueOf(input);
            BigDecimal rounded = inputBD.setScale(prec, mode);
            return rounded.doubleValue();

        } catch (NumberFormatException e) {
            return -1;
        }
    }

    public void clear() {
        this.up_f = 0;
        this.unknown_f = 0;
        this.down_f = 0;

        this.availability = 0;
        this.reliability = 0;
    }
    public void calcAR(Set<Entry<DateTime, Integer>> samples,DateTime runDate, int okSt, int warningSt, int unknownSt, int downSt, int missingSt) throws ParseException {
        int[] okstatusInfo = countStatusAppearancesInSecs(samples,okSt);
        int[] warningstatusInfo = countStatusAppearancesInSecs(samples,warningSt);
        int[] unknownstatusInfo = countStatusAppearancesInSecs(samples,unknownSt);
        int[] downstatusInfo =countStatusAppearancesInSecs(samples,downSt);
        int[] missingInfo = countStatusAppearancesInSecs(samples,missingSt);
        int daySeconds = Utils.calcDaySeconds(runDate, runDate);
        int[] upstatusInfo = new int[2];
        upstatusInfo[0] = okstatusInfo[0] + warningstatusInfo[0];
        upstatusInfo[1] = okstatusInfo[1] + warningstatusInfo[1];

        double knownPeriod = (double) daySeconds - (double) unknownstatusInfo[1] - (double) missingInfo[1];
        double knownScheduled = knownPeriod - (double) downstatusInfo[1];
        double minutesAvail = ((double) upstatusInfo[1] / (double) knownPeriod) * 100;
        if (Double.valueOf(minutesAvail).isNaN()) {
            minutesAvail = -1;
        }
        double minutesRel = ((double) upstatusInfo[1] / knownScheduled) * 100;

        if (Double.valueOf(minutesRel).isNaN()) {
            minutesRel = -1;
        }

        double upT = ((double) upstatusInfo[1]) / (double) daySeconds;
        this.availability = round(minutesAvail, 5, BigDecimal.ROUND_HALF_UP);
        this.reliability = round(minutesRel, 5, BigDecimal.ROUND_HALF_UP);
        this.up_f = round(((double) upstatusInfo[1] / (double) daySeconds), 5, BigDecimal.ROUND_HALF_UP);
        this.unknown_f = round((((double) unknownstatusInfo[1] + (double) missingInfo[1]) / (double) daySeconds), 5, BigDecimal.ROUND_HALF_UP);
        this.down_f = round(((double) downstatusInfo[1] / (double) daySeconds), 5, BigDecimal.ROUND_HALF_UP);

    }

    /**
     * Calculates the times a specific status appears on the timeline
     *
     * @param status , the status to calculate the appearances
     * @return , the num of the times the specific status appears on the
     * timeline
     */
    private int[] countStatusAppearancesInSecs(Set<Entry<DateTime, Integer>>  samples,int status) throws ParseException {

        int[] statusInfo = new int[2];
        int count = 0;
        ArrayList<DateTime[]> durationTimes = new ArrayList<>();
        DateTime startDt = null;
        DateTime endDt = null;

        boolean added = true;
        for (Map.Entry<DateTime, Integer> entry : samples) {
            if (status == entry.getValue()) {
                startDt = entry.getKey();
                count++;
                added = false;
            } else {
                if (!added) {
                    endDt = entry.getKey();

                    DateTime[] statusDur = new DateTime[2];
                    statusDur[0] = startDt;
                    statusDur[1] = endDt;
                    durationTimes.add(statusDur);
                    startDt = null;
                    endDt = null;
                    added = true;
                }
            }

        }
        if (!added) {

            endDt = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", startDt.toDate(), 23, 59, 59).plusSeconds(1);

            DateTime[] statusDur = new DateTime[2];
            statusDur[0] = startDt;
            statusDur[1] = endDt;
            durationTimes.add(statusDur);

        }
        statusInfo[0] = count;
        statusInfo[1] = countStatusDurationInSecs(durationTimes);
        return statusInfo;

    }

    /**
     * Calculates the times a specific status appears on the timeline
     *
     * @param status , the status to calculate the appearances
     * @return , the num of the times the specific status appears on the
     * timeline
     */
    public int[] countStatusAppearances(Set<Entry<DateTime, Integer>> samples,int status) throws ParseException {
        int[] statusInfo = new int[2];
        int count = 0;
        ArrayList<DateTime[]> durationTimes = new ArrayList<>();
        DateTime startDt = null;
        DateTime endDt = null;

        boolean added = true;
        for (Map.Entry<DateTime, Integer> entry : samples) {
            if (status == entry.getValue()) {
                startDt = entry.getKey();
                count++;
                added = false;
            } else {
                if (!added) {
                    endDt = entry.getKey();

                    DateTime[] statusDur = new DateTime[2];
                    statusDur[0] = startDt;
                    statusDur[1] = endDt;
                    durationTimes.add(statusDur);
                    startDt = null;
                    endDt = null;
                    added = true;
                }
            }

        }
        if (!added) {

            endDt = Utils.createDate("yyyy-MM-dd'T'HH:mm:ss'Z'", startDt.toDate(), 23, 59, 59);

            DateTime[] statusDur = new DateTime[2];
            statusDur[0] = startDt;
            statusDur[1] = endDt;
            durationTimes.add(statusDur);

        }
        statusInfo[0] = count;
        statusInfo[1] = countStatusDuration(durationTimes);
        return statusInfo;

    }

    /**
     * Calculates the total duration in mins of a status appearance
     *
     * @param durationTimes
     * @return
     */
    public int countStatusDuration(ArrayList<DateTime[]> durationTimes) throws ParseException {

        int minutesInt = 0;
        for (DateTime[] dt : durationTimes) {
            DateTime startDt = dt[0];
            DateTime endDt = dt[1];
            long startL = startDt.getMillis();
            long endL = endDt.getMillis();

            long total = endL - startL;
            int min = (int) TimeUnit.MILLISECONDS.toMinutes(total);

            //Minutes minutes = Minutes.minutesBetween(startDt, (endDt.plusMinutes(1)));
            minutesInt = minutesInt + min;
        }
        return minutesInt + 1;
    }

    /**
     * Calculates the total duration in secs of a status appearance
     *
     * @param durationTimes
     * @return
     */
    public int countStatusDurationInSecs(ArrayList<DateTime[]> durationTimes) throws ParseException {

        int secsInt = 0;
        for (DateTime[] dt : durationTimes) {
            DateTime startDt = dt[0];
            DateTime endDt = dt[1];
            long startL = startDt.getMillis();
            long endL = endDt.getMillis();

            long total = endL - startL;
            int secs = (int) TimeUnit.MILLISECONDS.toSeconds(total);

            //Minutes minutes = Minutes.minutesBetween(startDt, (endDt.plusMinutes(1)));
            secsInt = secsInt + secs;
        }
        return secsInt;
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

    public double getUp_f() {
        return up_f;
    }

    public void setUp_f(double up_f) {
        this.up_f = up_f;
    }

    public double getUnknown_f() {
        return unknown_f;
    }

    public void setUnknown_f(double unknown_f) {
        this.unknown_f = unknown_f;
    }

    public double getDown_f() {
        return down_f;
    }

    public void setDown_f(double down_f) {
        this.down_f = down_f;
    }


}
