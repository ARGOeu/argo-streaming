package argo.functions.calctrends;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import argo.pojos.Timeline;
import argo.functions.calctimelines.TimelineMerger;
import argo.pojos.EndpointTrends;
import argo.pojos.MetricTrends;
import argo.profiles.OperationsParser;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author cthermolia
 *
 * CalcEndpointTrends, count status changes for each service endpoint
 * group
 */
public class CalcEndpointFlipFlopTrends extends RichGroupReduceFunction<MetricTrends, EndpointTrends> {

    private OperationsParser operationsParser;
    private String operation;

    public CalcEndpointFlipFlopTrends(String operation, OperationsParser operationsParser) {
        this.operation = operation;
        this.operationsParser = operationsParser;
    }
/**
 * 
 * @param in, a collection of MetricTrends as calculated on previous steps , from group, service, endpoint, metric groups
 * @param out, a collection of EndpointTrends containing the information of the computation on group ,service, endpoint groups
 * @throws Exception 
 */
    @Override
    public void reduce(Iterable<MetricTrends> in, Collector< EndpointTrends> out) throws Exception {
        String group = null;
        String service = null;
        String hostname = null;
        ArrayList<EndpointTrends> list = new ArrayList<>();
       
       //store the necessary info
       //collect all timelines in a list
        ArrayList<Timeline> timelinelist = new ArrayList<>();
        for (MetricTrends time : in) {
            group = time.getGroup();
            service = time.getService();
            hostname = time.getEndpoint();
            Timeline timeline = time.getTimeline();
            timelinelist.add(timeline);
        }
        // merge the timelines into one timeline ,  
        // as multiple status (each status exist in each timeline) correspond to each timestamp, there is a need to conclude into one status/timestamp
        //for each timestamp the status that prevails is concluded by the truth table that is defined for the operation
        
        TimelineMerger timelinemerger = new TimelineMerger(operation, operationsParser);

        Timeline mergedTimeline = timelinemerger.mergeTimelines(timelinelist); //collect all timelines that correspond to the group service endpoint group , merge them in order to create one timeline
        Integer flipflops = mergedTimeline.calculateStatusChanges();//calculate flip flops on the concluded merged timeline

        EndpointTrends endpointTrends = new EndpointTrends(group, service, hostname, mergedTimeline, flipflops);
        out.collect(endpointTrends);
    }

}
