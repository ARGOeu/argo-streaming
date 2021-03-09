package argo.functions.servicetrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.ServiceEndpTimelinePojo;
import argo.pojos.ServiceFlipFlopPojo;
import argo.utils.Utils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import parsers.OperationsParser;

/**
 *
 * @author cthermolia
 *
 * CalcServiceEndpointFlipFlop, count status changes for each service endpoint
 * group
 */
public class CalcServiceFlipFlop implements GroupReduceFunction< ServiceEndpTimelinePojo, ServiceFlipFlopPojo> {

    private HashMap<String, String> serviceOperations;
    private HashMap<String, HashMap<String, String>> opTruthTable;
    private String profileName;
    private ArrayList<String> profileServices;

    public CalcServiceFlipFlop(HashMap<String, String> serviceOperations, HashMap<String, HashMap<String, String>> opTruthTable, String profileName, ArrayList<String> profileServices) {
        this.serviceOperations = serviceOperations;
        this.opTruthTable = opTruthTable;
        this.profileName = profileName;
        this.profileServices = profileServices;
    }

    @Override
    public void reduce(Iterable<ServiceEndpTimelinePojo> in, Collector< ServiceFlipFlopPojo> out) throws Exception {
        String group = null;
        String service = null;
        ArrayList<Date> timeline = new ArrayList<>();
        ArrayList<ServiceEndpTimelinePojo> list = new ArrayList<>();
        HashMap<String, String> truthTable = new HashMap<>();
        //construct a timeline containing all the timestamps of each metric timeline
        int i = 0;
        for (ServiceEndpTimelinePojo t : in) {
            service = t.getService();
            group = t.getGroup();
           
            TreeMap<String, String> metricTimeline = t.getTimelineMap();
            for (String time : metricTimeline.keySet()) {
                Date dt = Utils.convertStringToDate(time);
                timeline.add(dt);
            }

            String serviceOperation = serviceOperations.get(service);
            truthTable = opTruthTable.get(serviceOperation);
            list.add(t);

            i++;
        }

        TreeMap<Date, ArrayList<String>> statusMap = createStatusTimeline(timeline, list);
        TreeMap<String, String> resultMap = operateStatus(statusMap, truthTable);
        int flipflops = calcFlipFlops(resultMap);

        ServiceFlipFlopPojo servEndpFlipFlop = new ServiceFlipFlopPojo(profileName, group, service, flipflops);

        //Tuple4<String, String, String, Integer> tuple = new Tuple4<String, String, String, Integer>(group, service, hostname, flipflops);
        out.collect(servEndpFlipFlop);

    }

    /**
     * for each timestamp in the overall timeline , create a status list
     * containing all the statuses of each metric timeline that corresponds to
     * the timestamp of the overall timeline
     *
     * @param timeline
     * @param in
     * @return
     */
    private TreeMap<Date, ArrayList<String>> createStatusTimeline(ArrayList<Date> timeline, ArrayList<ServiceEndpTimelinePojo> in) throws ParseException {
        TreeMap<Date, ArrayList<String>> statusMap = new TreeMap<>();

        for (Date time : timeline) { //for each timestamp  T in the overall timeline
            String dt = Utils.convertDateToString(time);
            for (ServiceEndpTimelinePojo t : in) { // for each metric timeline

                TreeMap<String, String> metricTimeline = t.getTimelineMap();
                if (metricTimeline.containsKey(dt)) { // if timestamp T is in the metric timeline
                    ArrayList<String> statusList = statusMap.get(time);

                    if (statusList == null) {
                        statusList = new ArrayList<>();
                    }
                    String status = metricTimeline.get(dt); //get metric timeline status of T
                    statusList.add(status); //add status in overall timeline bucket  <T,{status1, ....} >
                    statusMap.put(time, statusList);

                } else { // if timestamp T is not in the metric timeline, parse each timestamp in metric timeline and add in overall timeline bucket the status of the timestamp that is previous of the timestamp> T
                    String previousStatus = null;
                    Date previousDate = null;
                    boolean statusadded = false;

                    for (String timestamp : metricTimeline.keySet()) { //for each timestamp in metric timeline
                        boolean isafter = false;
                        Date dtTimestamp = Utils.convertStringToDate(timestamp);
                        if (dtTimestamp.after(time)) { //is the timestamp after T
                            isafter = true;
                        }
                        if (isafter) { //if the timestamp is after T
                            ArrayList<String> statusList = statusMap.get(time);

                            if (statusList == null) {
                                statusList = new ArrayList<>();
                            }
                            statusList.add(previousStatus); //add previous timestamp status in  overall timeline bucket  <T,{status1, status2....} >
                            statusMap.put(time, statusList);
                            statusadded = true; // flag to show that the status is addedin status bucket
                        } else {//if the timestamp is before T , update previous status to store timestamp's status and continue to loop
                            previousDate = dtTimestamp;
                            previousStatus = metricTimeline.get(Utils.convertDateToString(previousDate));
                        }
                    }
                    if (!statusadded) {  // if the final timestamp is not added in status bucket add status in  overall timeline bucket  <T,{status1, status2....} >
                        //-(in case T is after the last timestamp of the metric timeline)
                        ArrayList<String> statusList = statusMap.get(time);

                        if (statusList == null) {
                            statusList = new ArrayList<>();
                        }
                        statusList.add(previousStatus);
                        statusMap.put(time, statusList);
                    }
                }
            }
        }
        return statusMap;
    }

    /**
     *
     * @param timelineStatusMap , the timeline status bucket containing all
     * statuses for each timestamp
     * @return , a timeline with one status per timestamp , extracted from the
     * operation's truth table
     * @throws ParseException
     */
    private TreeMap<String, String> operateStatus(TreeMap<Date, ArrayList<String>> timelineStatusMap, HashMap<String, String> truthTable) throws ParseException {

        TreeMap<String, String> result = new TreeMap<String, String>();

        for (Date dt : timelineStatusMap.keySet()) {
            String dateStr = Utils.convertDateToString(dt);

            ArrayList<String> statusList = timelineStatusMap.get(dt);

            String finalStatus = null;
            int pos = 0;

            Iterator<String> iter = statusList.iterator();
            while (iter.hasNext()) {
                if (pos == 0) {
                    finalStatus = iter.next();
                } else {
                    String status = iter.next();
                    String key = finalStatus + "-" + status;
                    if (truthTable.containsKey(key)) {
                        finalStatus = truthTable.get(key);
                    } else { //reverse status combination
                        key = status + "-" + finalStatus;
                        finalStatus = truthTable.get(key);
                    }
                }
                pos++;
            }
            result.put(dateStr, finalStatus);
        }
        return result;
    }

    private int calcFlipFlops(TreeMap<String, String> map) {

        String previousStatus = null;
        int flipflop = 0;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String status = entry.getValue();
            if (previousStatus != null && status != null && !status.equalsIgnoreCase(previousStatus)) {
                flipflop++;
            }
            previousStatus = status;
        }
        return flipflop;
    }

}
