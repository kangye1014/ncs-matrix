package com.cubead.performance.martix;

public class QuotaWithValue {

    private Quota quota;

    private Double value;

    public Quota getQuota() {
        return quota;
    }

    public void setQuota(Quota quota) {
        this.quota = quota;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public QuotaWithValue(Quota quota, Double value) {
        super();
        this.quota = quota;
        this.value = value;
    }

    public QuotaWithValue(Quota quota) {
        super();
        this.quota = quota;
    }
}
