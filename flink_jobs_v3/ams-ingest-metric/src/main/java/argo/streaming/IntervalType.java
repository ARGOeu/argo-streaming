package argo.streaming;

public enum IntervalType {

    DAY("d"),
    HOURS("h"), MINUTES("m");
    public final String value;

    private IntervalType(String value) {
        this.value = value;
    }
}