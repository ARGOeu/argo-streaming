/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.batch;

import java.io.Serializable;
import java.util.ArrayList;
import timelines.Timeline;

/**
 *
 * StatusMetricTimeline
 */
public class StatusMetricTimeline implements Serializable{

    private String group;
    private String service;
    private String hostname;
    private String metric;
    private ArrayList<StatusMetric> statusMetrics;
    Timeline timeline;

    public StatusMetricTimeline() {
        group="";
        service="";
        hostname="";
        statusMetrics=new ArrayList<>();
        timeline=new Timeline();
    }

    public StatusMetricTimeline(String group, String service, String hostname, String metric, ArrayList<StatusMetric> statusMetrics, Timeline timeline) {
        this.group = group;
        this.service = service;
        this.hostname = hostname;
        this.metric = metric;
        this.statusMetrics = statusMetrics;
        this.timeline = timeline;
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

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public ArrayList<StatusMetric> getStatusMetrics() {
        return statusMetrics;
    }

    public void setStatusMetrics(ArrayList<StatusMetric> statusMetrics) {
        this.statusMetrics = statusMetrics;
    }

    public Timeline getTimeline() {
        return timeline;
    }

    public void setTimeline(Timeline timeline) {
        this.timeline = timeline;
    }
    
    
    
    

}
