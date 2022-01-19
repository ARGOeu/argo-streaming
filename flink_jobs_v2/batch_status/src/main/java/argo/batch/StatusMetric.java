package argo.batch;

import java.util.Objects;

public class StatusMetric {

    private String group;
    private String function;
    private String service;
    private String hostname;
    private String metric;
    private String status;
    private String timestamp;
    private int dateInt;
    private int timeInt;
    private String summary;
    private String message;
    private String prevState;
    private String prevTs;
    private String actualData;
    private String ogStatus; // original status from moniting host
    private String ruleApplied; // threshold rule applied - empty if not 
    private String info; // extra endpoint information provided by the topology
    private String tags;
    private boolean hasThr;

    public StatusMetric() {
        this.group = "";
        this.function = "";
        this.service = "";
        this.hostname = "";
        this.metric = "";
        this.status = "";
        this.timestamp = "";
        this.dateInt = 0;
        this.timeInt = 0;
        this.summary = "";
        this.message = "";
        this.prevState = "";
        this.prevTs = "";
        this.actualData = "";
        this.ogStatus = "";
        this.ruleApplied = "";
        this.info = "";
        this.tags = "";
        this.hasThr = false;
    }

    public StatusMetric(String group, String function, String service, String hostname, String metric, String status, String timestamp,
            int dateInt, int timeInt, String summary, String message, String prevState, String prevTs, String actualData, String ogStatus, String ruleApplied, String info, String tags) {

        this.group = group;
        this.function = function;
        this.service = service;
        this.hostname = hostname;
        this.metric = metric;
        this.status = status;
        this.timestamp = timestamp;
        this.dateInt = dateInt;
        this.timeInt = timeInt;
        this.summary = summary;
        this.message = message;
        this.prevState = prevState;
        this.prevTs = prevTs;
        this.actualData = actualData;
        this.ogStatus = ogStatus;
        this.ruleApplied = ruleApplied;
        this.info = info;
        this.tags = tags;
    }

    public boolean getHasThr() {
        return hasThr;
    }

    public void setHasThr(boolean hasThr) {
        this.hasThr = hasThr;
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

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getDateInt() {
        return dateInt;
    }

    public void setDateInt(int dateInt) {
        this.dateInt = dateInt;
    }

    public int getTimeInt() {
        return timeInt;
    }

    public void setTimeInt(int timeInt) {
        this.timeInt = timeInt;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getPrevState() {
        return prevState;
    }

    public void setPrevState(String prevState) {
        this.prevState = prevState;
    }

    public String getPrevTs() {
        return prevTs;
    }

    public void setPrevTs(String prevTs) {
        this.prevTs = prevTs;
    }

    public String getActualData() {
        return actualData;
    }

    public void setActualData(String actualData) {
        this.actualData = actualData;
    }

    public String getOgStatus() {
        return ogStatus;
    }

    public void setOgStatus(String ogStatus) {
        this.ogStatus = ogStatus;
    }

    public String getRuleApplied() {
        return ruleApplied;
    }

    public void setRuleApplied(String ruleApplied) {
        this.ruleApplied = ruleApplied;
    }

    public String getInfo() {
        return this.info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public String toString() {
        return "(" + this.group + "," + this.service + "," + this.hostname + "," + this.metric + "," + this.status + "," + this.timestamp + ","
                + this.dateInt + "," + this.timeInt + "," + this.prevState + "," + this.prevTs + "," + this.actualData + "," + this.ogStatus + "," + this.ruleApplied + "," + this.info + "," + this.tags + ")";
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
        final StatusMetric other = (StatusMetric) obj;
        if (this.dateInt != other.dateInt) {
            return false;
        }
        if (this.timeInt != other.timeInt) {
            return false;
        }
        if (this.hasThr != other.hasThr) {
            return false;
        }
        if (!Objects.equals(this.group, other.group)) {
            return false;
        }
        if (!Objects.equals(this.function, other.function)) {
            return false;
        }
        if (!Objects.equals(this.service, other.service)) {
            return false;
        }
        if (!Objects.equals(this.hostname, other.hostname)) {
            return false;
        }
        if (!Objects.equals(this.metric, other.metric)) {
            return false;
        }
        if (!Objects.equals(this.status, other.status)) {
            return false;
        }
        if (!Objects.equals(this.timestamp, other.timestamp)) {
            return false;
        }
        if (!Objects.equals(this.summary, other.summary)) {
            return false;
        }
        if (!Objects.equals(this.message, other.message)) {
            return false;
        }
        if (!Objects.equals(this.prevState, other.prevState)) {
            return false;
        }
        if (!Objects.equals(this.prevTs, other.prevTs)) {
            return false;
        }
        if (!Objects.equals(this.actualData, other.actualData)) {
            return false;
        }
        if (!Objects.equals(this.ogStatus, other.ogStatus)) {
            return false;
        }
        if (!Objects.equals(this.ruleApplied, other.ruleApplied)) {
            return false;
        }
        if (!Objects.equals(this.info, other.info)) {
            return false;
        }
        return Objects.equals(this.tags, other.tags);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + Objects.hashCode(this.group);
        hash = 59 * hash + Objects.hashCode(this.function);
        hash = 59 * hash + Objects.hashCode(this.service);
        hash = 59 * hash + Objects.hashCode(this.hostname);
        hash = 59 * hash + Objects.hashCode(this.metric);
        hash = 59 * hash + Objects.hashCode(this.status);
        hash = 59 * hash + Objects.hashCode(this.timestamp);
        hash = 59 * hash + this.dateInt;
        hash = 59 * hash + this.timeInt;
        hash = 59 * hash + Objects.hashCode(this.summary);
        hash = 59 * hash + Objects.hashCode(this.message);
        hash = 59 * hash + Objects.hashCode(this.prevState);
        hash = 59 * hash + Objects.hashCode(this.prevTs);
        hash = 59 * hash + Objects.hashCode(this.actualData);
        hash = 59 * hash + Objects.hashCode(this.ogStatus);
        hash = 59 * hash + Objects.hashCode(this.ruleApplied);
        hash = 59 * hash + Objects.hashCode(this.info);
        hash = 59 * hash + Objects.hashCode(this.tags);
        hash = 59 * hash + (this.hasThr ? 1 : 0);
        return hash;
    }
    
    

}
