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
public class MetricTimelinePojo {

    String group;
    String service;
    String endpoint;
    String metric;
    TreeMap<Date, String> timelineMap;

    public MetricTimelinePojo() {
    }

    
    public MetricTimelinePojo(String group, String service, String endpoint, String metric, TreeMap<Date, String> timelineMap) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.timelineMap = timelineMap;
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

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public TreeMap<Date, String> getTimelineMap() {
        return timelineMap;
    }

    public void setTimelineMap(TreeMap<Date, String> timelineMap) {
        this.timelineMap = timelineMap;
    }

}
