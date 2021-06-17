/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

import timelines.Timeline;

/**
 * ServiceTrends, describes the computed trend information extracted from the
 * timelines at the level of group service endpoints groups
 */
public class ServiceTrends {

    String group;
    String service;
    Timeline timeline;
    Integer flipflops;
    String function;

    public ServiceTrends() {
    }

    public ServiceTrends(String group, String service, Timeline timeline, Integer flipflops) {
        this.group = group;
        this.service = service;
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

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

}
