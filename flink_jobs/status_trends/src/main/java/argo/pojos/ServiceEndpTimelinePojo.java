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
 */
public class ServiceEndpTimelinePojo {

    private String group;
    private String service;
    private String endpoint;

    private TreeMap<String, String> timelineMap;

    public ServiceEndpTimelinePojo() {
    }

    
    public ServiceEndpTimelinePojo(String group, String service, String endpoint, TreeMap<String, String> timelineMap) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.timelineMap = timelineMap;
    }

    public String getGroup() {
        return group;
    }

    public String getService() {
        return service;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public TreeMap<String, String> getTimelineMap() {
        return timelineMap;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void setService(String service) {
        this.service = service;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setTimelineMap(TreeMap<String, String> timelineMap) {
        this.timelineMap = timelineMap;
    }

}
