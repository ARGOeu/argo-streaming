/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

import java.util.Date;
import java.util.TreeMap;

/**
 *
 * @author cthermolia
 * 
 * EndpointTrends, describes the computed trend information extracted from the timelines at the level of group service endpoints groups 
 */
public class EndpointTrends {

    String group;
    String service;
    String endpoint;
    Timeline timeline;
    Integer flipflops;

    public EndpointTrends() {
    }

    public EndpointTrends(String group, String service, String endpoint, Timeline timeline, Integer flipflops) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.timeline = timeline;
        this.flipflops = flipflops;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Timeline getTimeline() {
        return timeline;
    }

    public void setTimeline(Timeline timeline) {
        this.timeline = timeline;
    }

    public Integer getFlipflops() {
        return flipflops;
    }

    public void setFlipflops(Integer flipflops) {
        this.flipflops = flipflops;
    }

  

}
