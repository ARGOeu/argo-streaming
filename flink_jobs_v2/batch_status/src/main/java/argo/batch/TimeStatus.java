/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.batch;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public int hashCode() {
        int hash = 7;
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
        final TimeStatus other = (TimeStatus) obj;
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return Objects.equals(this.status, other.status);
    }

}
