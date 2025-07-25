package trends.calculations;

import java.util.Objects;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
public class Trends {

    private String group;
    private String service;
    private String endpoint;
    private String metric;
    private int flipflop;

    private String status;
    private int trends;
    private int duration;
    private String tags;

    public Trends(String group, String service, String endpoint, String metric, String status, int trends, int duration, String tags) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.status = status;
        this.trends = trends;
        this.duration = duration;
        this.tags = tags;
    }

    public Trends(String group, String service, String endpoint, String metric, String status, int trends) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.flipflop = flipflop;
        this.status = status;
        this.trends = trends;
    }

    public Trends(String group, String service, String endpoint, String metric, int flipflop,String tags) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.flipflop = flipflop;
        this.tags = tags;
    }

    public Trends(String group, int flipflop) {
        this.group = group;
        this.flipflop = flipflop;
    }

    public Trends(String group, String service, int flipflop) {
        this.group = group;
        this.service = service;
        this.flipflop = flipflop;
    }

    public Trends(String group, String service, String endpoint, int flipflop) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.flipflop = flipflop;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
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

    public int getFlipflop() {
        return flipflop;
    }

    public void setFlipflop(int flipflop) {
        this.flipflop = flipflop;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getTrends() {
        return trends;
    }

    public void setTrends(int trends) {
        this.trends = trends;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "(" + this.group + "," + this.service + "," + this.endpoint + "," + this.metric + "," + this.status + "," + this.flipflop + ","
                + this.status + "," + this.duration + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Trends)) return false;
        Trends trends1 = (Trends) o;
        return flipflop == trends1.flipflop && trends == trends1.trends && duration == trends1.duration && Objects.equals(group, trends1.group) && Objects.equals(service, trends1.service) && Objects.equals(endpoint, trends1.endpoint) && Objects.equals(metric, trends1.metric) && Objects.equals(status, trends1.status) && Objects.equals(tags, trends1.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, service, endpoint, metric, flipflop, status, trends, duration, tags);
    }
}
