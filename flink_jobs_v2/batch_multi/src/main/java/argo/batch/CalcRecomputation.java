package argo.batch;

import java.util.List;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;

import argo.avro.MetricProfile;

import java.io.IOException;
import java.text.ParseException;

import org.joda.time.DateTime;
import profilesmanager.MetricProfileManager;
import profilesmanager.OperationsManager;
import profilesmanager.RecomputationsManager;
import utils.Utils;

//transforms the original status to the excluded status in the case the metric is inside the exclude period
public class CalcRecomputation extends RichGroupReduceFunction<StatusMetric, StatusMetric> {

    private static final long serialVersionUID = 1L;

    final ParameterTool params;

    public CalcRecomputation(ParameterTool params) {
        this.params = params;
    }

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);
    private List<MetricProfile> mps;
    private List<GroupEndpoint> egp;
    private List<GroupGroup> ggp;
    private MetricProfileManager mpsMgr;
    private String runDate;
    private List<String> rec;
    private RecomputationsManager recMgr;
    private List<String> ops;
    private OperationsManager opsMgr;

    @Override
    public void open(Configuration parameters) throws IOException, ParseException {
        // Get data from broadcast variable
        this.runDate = params.getRequired("run.date");

        this.mps = getRuntimeContext().getBroadcastVariable("mps");
        this.rec = getRuntimeContext().getBroadcastVariable("rec");
        this.ops = getRuntimeContext().getBroadcastVariable("ops");

        // Initialize metric profile manager
        this.mpsMgr = new MetricProfileManager();
        this.mpsMgr.loadFromList(mps);
        // Initialize endpoint group manager
        this.recMgr = new RecomputationsManager();
        this.recMgr.loadJsonString(rec);
        this.opsMgr = new OperationsManager();
        this.opsMgr.loadJsonString(ops);

    }

    @Override
    public void reduce(Iterable<StatusMetric> in, Collector<StatusMetric> out) throws Exception {
        boolean startIsAdded = false; //keeps a notice if the start of the exclude period is added so not to be added again
        boolean insertExcludedStart = false;//gives an order to add a new item at the start of the exclude period
        boolean insertExcludedEnd = false; //gives an order to add a new item at the end of the exclude period
        boolean endIsAdded = false;
        StatusMetric lastItem = null;
        String lastOriginalStatus = "";
        //DateTime beginExclude=new DateTime();
        DateTime endExclude = new DateTime();

        for (StatusMetric item : in) {
            lastItem = item; //we keep the item
            lastOriginalStatus = item.getStatus(); //we keep the original status of the timestamp

            if (!item.getTimestamp().split("T")[0].equalsIgnoreCase(this.runDate)) {
                out.collect(item);
                continue;
            }

            RecomputationsManager.ExcludedMetric excludedMetric = this.recMgr.findMetricExcluded(
                    item.getGroup(), item.getService(), item.getHostname(), item.getMetric());

            if (excludedMetric == null) { //the metric does not belong to exclude perios. it is collected as it is
                out.collect(item);
                continue;
            }

            // Get the exclusion period
            ExclusionPeriod exclusionPeriod = setupExclusionPeriod(excludedMetric); //the exclude period is defined inside today if it is extended to more days
            DateTime beginExclude = exclusionPeriod.getBeginExclude();
            endExclude = exclusionPeriod.getEndExclude();

            DateTime timestamp = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", item.getTimestamp());
            boolean isInRecomputation = !timestamp.isBefore(beginExclude); //if the timestamp of the item is after the start of the exclude period

            if (isInRecomputation && !startIsAdded && timestamp.isAfter(beginExclude)) { //if the item is after the start of exclude period but it is not same as the start of the exclude period and also is not yet added as first element
                insertExcludedStart = true; //we should add an extra element to define that the exclude period starts
            }

            if (!timestamp.isBefore(endExclude)) { //if timestamp is after or equal the end of exclude period (plus 1 minute)
                isInRecomputation = false; // the element is not in the exclude period
                insertExcludedStart = false;
                if (!endIsAdded && timestamp.isAfter(endExclude)) { //if the timestamp is after the end of exclude period (plus 1 minute)
                    insertExcludedEnd = true; //an extra item should be added to define that the original period starts and the status is as original after the exclude period ends
                }
            }

            if (isInRecomputation) { //if the timestamp is in recomputation
                if (insertExcludedStart) { //insert the extra element to start
                    out.collect(createExcludedMetric(item, beginExclude));
                    insertExcludedStart = false;
                }
                item.setStatus(this.opsMgr.getDefaultExcludedState()); //set the initial element to exclude status
                out.collect(item);
                startIsAdded = true; // define that element of start is added so we dont need to add it again
            } else { //exclude period has ended
                if (insertExcludedEnd) { //add an extra element to define the end of period
                    out.collect(createEndExcludedMetric(item, endExclude, lastOriginalStatus)); //adds an element the next minute of end period with status the original status of the previous element
                    insertExcludedEnd = false;
                    endIsAdded=true;
                }
                out.collect(item);
            }
        }

        if (!endIsAdded) {//if no timestamps are received after the end of the exclude period , the end of the exclude period should be added as an element to define that original period starts
            out.collect(createEndExcludedMetric(lastItem, endExclude, lastOriginalStatus));
        }
    }

    private ExclusionPeriod setupExclusionPeriod(RecomputationsManager.ExcludedMetric excludedMetric) throws ParseException {
        DateTime today = Utils.convertStringtoDate("yyyy-MM-dd", runDate).withTime(0, 0, 0, 0);
        DateTime tomorrow = today.plusDays(1);

        DateTime startPeriod = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", excludedMetric.getStartPeriod());
        DateTime endPeriod = Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", excludedMetric.getEndPeriod());

        DateTime beginExclude = startPeriod.isBefore(today) ? today : startPeriod;
        DateTime endExclude = endPeriod.isAfter(tomorrow) ? tomorrow : endPeriod.plusMinutes(1);

        return new ExclusionPeriod(beginExclude, endExclude);
    }


    private StatusMetric createExcludedMetric(StatusMetric item, DateTime beginExclude) throws ParseException {
        StatusMetric excludedItem = cloneItem(item);
        excludedItem.setStatus(this.opsMgr.getDefaultExcludedState());
        excludedItem.setTimestamp(Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", beginExclude));
        setDateAndTimeInts(excludedItem);
        return excludedItem;
    }

    private StatusMetric createEndExcludedMetric(StatusMetric item, DateTime endExclude, String originalStatus) throws ParseException {
        StatusMetric endExcludedItem = cloneItem(item);
        endExcludedItem.setStatus(originalStatus);
        endExcludedItem.setTimestamp(Utils.convertDateToString("yyyy-MM-dd'T'HH:mm:ss'Z'", endExclude));
        setDateAndTimeInts(endExcludedItem);
        return endExcludedItem;
    }

    private void setDateAndTimeInts(StatusMetric item) {
        String timestamp = item.getTimestamp().split("Z")[0];
        String[] tsToken = timestamp.split("T");
        item.setDateInt(Integer.parseInt(tsToken[0].replace("-", "")));
        item.setTimeInt(Integer.parseInt(tsToken[1].replace(":", "")));
    }


    private StatusMetric cloneItem(StatusMetric item) {
        StatusMetric startExludedPeriodItem = new StatusMetric();
        startExludedPeriodItem.setOgStatus(item.getOgStatus());
        startExludedPeriodItem.setActualData(item.getActualData());
        startExludedPeriodItem.setDateInt(item.getDateInt());
        startExludedPeriodItem.setFunction(item.getFunction());
        startExludedPeriodItem.setGroup(item.getGroup());
        startExludedPeriodItem.setHasThr(item.getHasThr());
        startExludedPeriodItem.setHostname(item.getHostname());
        startExludedPeriodItem.setInfo(item.getInfo());
        startExludedPeriodItem.setMessage(item.getMessage());
        startExludedPeriodItem.setMetric(item.getMetric());
        startExludedPeriodItem.setRuleApplied(item.getRuleApplied());
        startExludedPeriodItem.setService(item.getService());
        startExludedPeriodItem.setHostname(item.getHostname());
        startExludedPeriodItem.setTags(item.getTags());
        startExludedPeriodItem.setSummary(item.getSummary());
        startExludedPeriodItem.setPrevTs(item.getPrevTs());
        startExludedPeriodItem.setPrevState(item.getPrevState());
        startExludedPeriodItem.setTimeInt(item.getTimeInt());

        return startExludedPeriodItem;


    }

    class ExclusionPeriod {
        private final DateTime beginExclude;
        private final DateTime endExclude;

        public ExclusionPeriod(DateTime beginExclude, DateTime endExclude) {
            this.beginExclude = beginExclude;
            this.endExclude = endExclude;
        }

        public DateTime getBeginExclude() {
            return beginExclude;
        }

        public DateTime getEndExclude() {
            return endExclude;
        }
    }

}
