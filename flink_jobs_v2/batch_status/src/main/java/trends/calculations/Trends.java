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
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.group);
        hash = 53 * hash + Objects.hashCode(this.service);
        hash = 53 * hash + Objects.hashCode(this.endpoint);
        hash = 53 * hash + Objects.hashCode(this.metric);
        hash = 53 * hash + this.flipflop;
        hash = 53 * hash + Objects.hashCode(this.status);
        hash = 53 * hash + this.trends;
        hash = 53 * hash + this.duration;
        hash = 53 * hash + Objects.hashCode(this.tags);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Trends other = (Trends) obj;
        if (this.flipflop != other.flipflop) {
            return false;
        }
        if (this.trends != other.trends) {
            return false;
        }
        if (this.duration != other.duration) {
            return false;
        }
        if (!Objects.equals(this.group, other.group)) {
            return false;
        }
        if (!Objects.equals(this.service, other.service)) {
            return false;
        }
        if (!Objects.equals(this.endpoint, other.endpoint)) {
            return false;
        }
        if (!Objects.equals(this.metric, other.metric)) {
            return false;
        }
        if (!Objects.equals(this.status, other.status)) {
            return false;
        }
        return Objects.equals(this.tags, other.tags);
    }

}
