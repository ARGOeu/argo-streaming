package trends;

/**
 *
 * StatusTrends stores info about status trends
 */
public class StatusTrends {

    private String group;
    private String service;
    private String endpoint;
    private String metric;
    private String tags;
    private String status;
    private Integer statusFreq;
    private Integer statusDur;

    public StatusTrends(String group, String service, String endpoint, String metric, String tags, String status, Integer statusFreq, Integer statusDur) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.tags = tags;
        this.status = status;
        this.statusFreq = statusFreq;
        this.statusDur = statusDur;
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

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getStatusFreq() {
        return statusFreq;
    }

    public void setStatusFreq(Integer statusFreq) {
        this.statusFreq = statusFreq;
    }

    public Integer getStatusDur() {
        return statusDur;
    }

    public void setStatusDur(Integer statusDur) {
        this.statusDur = statusDur;
    }

}
