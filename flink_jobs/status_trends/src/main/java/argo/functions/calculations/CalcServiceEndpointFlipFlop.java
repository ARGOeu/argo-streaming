package argo.functions.calculations;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.utils.TimelineBuilder;
import argo.pojos.TimelineTrends;
import argo.profileparsers.OperationsParser;
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

/**
 *
 * @author cthermolia
 *
 * CalcServiceEndpointFlipFlop, count status changes for each service endpoint
 * group
 */
public class CalcServiceEndpointFlipFlop implements GroupReduceFunction< TimelineTrends, TimelineTrends> {

    private HashMap<String, String> opTruthTable;
    private OperationsParser operationsParser;

    public CalcServiceEndpointFlipFlop(OperationsParser operationsParser, String operation) {
        this.operationsParser = this.operationsParser;
        this.opTruthTable=operationsParser.getOpTruthTable().get(operation);
    }

    @Override
    public void reduce(Iterable<TimelineTrends> in, Collector< TimelineTrends> out) throws Exception {
        String group = null;
        String service = null;
        String hostname = null;
        ArrayList<TimelineTrends> list = new ArrayList<>();
        //construct a timeline containing all the timestamps of each metric timeline
         
        TimelineBuilder timebuilder = new TimelineBuilder();

        ArrayList<TimelineTrends> timelist = new ArrayList<>();
        for (TimelineTrends time : in) {
            group = time.getGroup();
            service = time.getService();
            hostname = time.getEndpoint();
            timelist.add(time);
        }

        TreeMap<Date, String> resultMap = timebuilder.buildStatusTimeline(timelist, opTruthTable);
        int flipflops = timebuilder.calcFlipFlops(resultMap);

        TimelineTrends servEndpFlipFlop = new TimelineTrends(group, service, hostname, resultMap, flipflops);

        //Tuple4<String, String, String, Integer> tuple = new Tuple4<String, String, String, Integer>(group, service, hostname, flipflops);
        out.collect(servEndpFlipFlop);

    }

   
   
}
