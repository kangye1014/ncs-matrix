package com.cubead.test.base;

import java.util.List;

import com.cubead.performance.martix.MatrixTableSearch.QuotaField;
import com.lmax.disruptor.EventFactory;

/**
 * distuptor基础事件
 * 
 * @author kangye
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
