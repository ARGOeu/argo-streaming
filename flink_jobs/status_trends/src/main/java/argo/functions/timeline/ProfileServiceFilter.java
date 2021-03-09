/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.functions.timeline;

import argo.pojos.ServiceEndpTimelinePojo;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cthermolia
 *
 * ProfileServiceFilter, filters data by status
 */
public class ProfileServiceFilter implements FilterFunction<ServiceEndpTimelinePojo> {

    static Logger LOG = LoggerFactory.getLogger(ProfileServiceFilter.class);
    private ArrayList<String> services;
    private String profileName;
    public ProfileServiceFilter(ArrayList<String> services, String name) {
        this.services = services;
        this.profileName=profileName;
    }
    //if the status field value in Tuple equals the given status returns true, else returns false

    @Override
    public boolean filter(ServiceEndpTimelinePojo t) throws Exception {
            boolean exists = false;
             
       for (String s : services) {
            if (s.equals(t.getService())) {
                exists = true;
                System.out.println("exists ---- "+s);
                 break;
            }
        }
        return exists;

    }
}
