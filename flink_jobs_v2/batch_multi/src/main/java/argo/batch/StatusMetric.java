package argo.batch;

import java.util.Objects;

public class StatusMetric  implements  Cloneable{

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
    public boolean equals(Object o) {
        if (!(o instanceof StatusMetric)) return false;
        StatusMetric that = (StatusMetric) o;
        return dateInt == that.dateInt && timeInt == that.timeInt && hasThr == that.hasThr && Objects.equals(group, that.group) && Objects.equals(function, that.function) && Objects.equals(service, that.service) && Objects.equals(hostname, that.hostname) && Objects.equals(metric, that.metric) && Objects.equals(status, that.status) && Objects.equals(timestamp, that.timestamp) && Objects.equals(summary, that.summary) && Objects.equals(message, that.message) && Objects.equals(prevState, that.prevState) && Objects.equals(prevTs, that.prevTs) && Objects.equals(actualData, that.actualData) && Objects.equals(ogStatus, that.ogStatus) && Objects.equals(ruleApplied, that.ruleApplied) && Objects.equals(info, that.info) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, function, service, hostname, metric, status, timestamp, dateInt, timeInt, summary, message, prevState, prevTs, actualData, ogStatus, ruleApplied, info, tags, hasThr);
    }

    @Override
    public StatusMetric clone() {
        try {
            return (StatusMetric) super.clone(); // shallow copy
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(); // should never happen if Cloneable is implemented
        }
    }
}
