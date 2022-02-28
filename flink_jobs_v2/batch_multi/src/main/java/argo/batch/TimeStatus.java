/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.batch;

import java.io.Serializable;

/**
 */
public class TimeStatus implements Serializable {

    private long timestamp;
    private Integer status;

    public TimeStatus() {
    }

    public TimeStatus(long timestamp, Integer status) {
        this.timestamp = timestamp;
        this.status = status;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "TimeStatus{" + "timestamp=" + timestamp + ", status=" + status + '}';
    }
}
