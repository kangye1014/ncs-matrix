package com.cubead.test.base;

import java.util.List;

import com.cubead.performance.martix.MatrixTableSearch.QuotaField;
import com.lmax.disruptor.EventFactory;

/**
 * WARNING: This is a mutable object which will be recycled by the RingBuffer.
 * You must take a copy of data it holds before the framework recycles it.
 */
public final class QutotaEvent {
    private List<QuotaField> value;

    public List<QuotaField> getValue() {
        return value;
    }

    public void setValue(List<QuotaField> value) {
        this.value = value;
    }

    public final static EventFactory<QutotaEvent> EVENT_FACTORY = new EventFactory<QutotaEvent>() {
        public QutotaEvent newInstance() {
            return new QutotaEvent();
        }
    };
}
