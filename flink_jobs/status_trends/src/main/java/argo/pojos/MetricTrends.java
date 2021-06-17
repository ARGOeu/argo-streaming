/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

import timelines.Timeline;

/**
 * MetricTrends, describes the computed trend information extracted from the set
 * of the timelines at the level of group service endpoints metrics groups
 */
public class MetricTrends {

    String group;
    String service;
    String endpoint;
    String metric;
    Timeline timeline;
    Integer flipflops;

    public MetricTrends() {
    }

    public MetricTrends(String group, String service, String hostname, String metric, timelines.Timeline timeline, Integer flipflop) {
        this.group = group;
        this.service = service;
        this.endpoint = hostname;
        this.metric = metric;
        this.timeline = timeline;
        this.flipflops = flipflop;
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
