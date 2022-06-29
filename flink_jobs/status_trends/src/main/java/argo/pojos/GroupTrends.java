/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

import timelines.Timeline;

/**
 * MetricTrends, describes the computed trend information extracted from the set
 * of the timelines at the level of group service endpoints metrics groups *
 */
public class GroupTrends {

    String group;

    Timeline timeline;
    Integer flipflops;

    public GroupTrends() {
    }

    public GroupTrends(String group, Timeline timeline, Integer flipflops) {
        this.group = group;
        this.timeline = timeline;
        this.flipflops = flipflops;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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
