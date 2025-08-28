package argo.streaming;

import org.apache.commons.lang.StringUtils;

public  class IntervalStruct {

    IntervalType intervalType;
    int intervalValue;

    public IntervalStruct(IntervalType intervalType, int intervalValue) {
        this.intervalType = intervalType;
        this.intervalValue = intervalValue;
    }

    public IntervalType getIntervalType() {
        return intervalType;
    }

    public void setIntervalType(IntervalType intervalType) {
        this.intervalType = intervalType;
    }

    public int getIntervalValue() {
        return intervalValue;
    }

    public void setIntervalValue(int intervalValue) {
        this.intervalValue = intervalValue;
    }

    public static IntervalStruct parseInterval(String intervalParam) {
        if (intervalParam == null) {
            return new IntervalStruct(IntervalType.DAY, 24); // default 1 day
        }

        String regex = "\\d+(h|d|m)$";
        if (!intervalParam.matches(regex)) {
            return new IntervalStruct(IntervalType.DAY, 24); // default
        }

        IntervalType intervalType;
        int intervalValue;

        if (intervalParam.endsWith("h")) {
            intervalType = IntervalType.HOURS;
            intervalValue = Integer.parseInt(intervalParam.replace("h", "")) * 60;
        } else if (intervalParam.endsWith("d")) {
            intervalType = IntervalType.DAY;
            intervalValue = Integer.parseInt(intervalParam.replace("d", "")) * 24 * 60;
        } else { // ends with m
            intervalType = IntervalType.MINUTES;
            intervalValue = Integer.parseInt(intervalParam.replace("m", ""));
        }

        return new IntervalStruct(intervalType, intervalValue);
    }

}
