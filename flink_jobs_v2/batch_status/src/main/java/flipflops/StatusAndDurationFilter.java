package flipflops;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StatusFilter, filters data by status and status appearances
 */
public class StatusAndDurationFilter implements FilterFunction<Tuple8<String, String, String, String, String, Integer, Integer,String>> {

    static Logger LOG = LoggerFactory.getLogger(StatusAndDurationFilter.class);
    private String status;

    public StatusAndDurationFilter(String status) {
        this.status = status;
    }
    //if the status field value in Tuple equals the given status  and status appearances>0 returns true, else returns false

    @Override
    public boolean filter(Tuple8<String, String, String, String, String,  Integer, Integer,String> t) throws Exception {

        if (t.f4.toString().equalsIgnoreCase(status) && t.f6 > 0) {
            return true;
        }
        return false;
    }

}
