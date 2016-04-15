package com.cubead.performance.martix;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;

/**
 * 合并后的结果集
 * 
 * @author kangye
 */
public class RowMergeResultSet {

    private Map<String, Double[]> rowQuotaSetMap = new ConcurrentHashMap<String, Double[]>();
    // 线程是尽量小,减少和DB查询线程抢占CPU
    private static ExecutorService executorService = Executors.newFixedThreadPool(2);

    public RowMergeResultSet() {
        super();
    }

    /**
     * 添加结果行,将其合并到
     * 
     * @author kangye
     * @param sqlRowResultMapping
     */
    public void addRowMergeResult(final SQLRowResultMapping sqlRowResultMapping) {

        executorService.execute(new Runnable() {
            public void run() {

                Thread.yield();

                final String key = sqlRowResultMapping.getDimension().parseAsKey();
                Double[] values = rowQuotaSetMap.get(key);

                if (null == values) {
                    // 新增
                    values = initZeroFullValues();
                }

                List<QuotaWithValue> quotaWithValues = sqlRowResultMapping.getQuotaWithValues();
                if (CollectionUtils.isEmpty(quotaWithValues))
                    throw new IllegalArgumentException("存入的sqlRowResultMapping不存在指标值");

                for (QuotaWithValue quotaWithValue : quotaWithValues) {
                    Integer seialNumber = quotaWithValue.getQuota().getSeialNumber();
                    values[seialNumber] += quotaWithValue.getValue();
                }

                rowQuotaSetMap.put(key, values);
            }
        });
    }

    public static Double[] initZeroFullValues() {

        Quota[] quotas = Quota.values();
        Double[] doubles = new Double[quotas.length];

        for (int i = 0; i < quotas.length; i++) {
            doubles[i] = 0.0;
        }

        return doubles;
    }

    public Map<String, Double[]> getRowQuotaSetMap() {
        return rowQuotaSetMap;
    }
}
