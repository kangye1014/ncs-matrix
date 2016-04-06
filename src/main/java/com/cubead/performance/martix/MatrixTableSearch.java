package com.cubead.performance.martix;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

@Component
public class MatrixTableSearch {

    private static final Logger logger = LoggerFactory.getLogger(MatrixTableSearch.class);

    ExecutorService executorService = Executors.newCachedThreadPool();

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * 维度
     */
    public class Dimension {

    }

    /**
     * 指标
     */
    public static enum Quota {
        PV, UV, CLICK, VISITORS
    }

    public class QuotaField {

        private Quota quota;

        private Double value;

        private Dimension dimension;

        public Dimension getDimension() {
            return dimension;
        }

        public void setDimension(Dimension dimension) {
            this.dimension = dimension;
        }

        public Quota getQuota() {
            return quota;
        }

        public Double getValue() {
            return value;
        }

        public QuotaField(Quota quota, Double value) {
            super();
            this.quota = quota;
            this.value = value;
        }

        @Override
        public String toString() {
            return "QuotaField [quota=" + quota + ", value=" + value + "]";
        }
    }

    class QuotaCalculationTask implements Callable<List<QuotaField>> {

        private String SQL;
        private Quota quota;
        private Dimension dimension;

        // public abstract List<QuotaField> calculatResult(String SQL, Quota
        // quota, Dimension dimension);

        public QuotaCalculationTask(String SQL, Quota quota, Dimension dimension) {
            this.SQL = SQL;
            this.quota = quota;
            this.dimension = dimension;
        }

        // 计算某个维度查询实现
        @Override
        public List<QuotaField> call() throws Exception {

            CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                    executorService);
            final String tableName = "ca_summary_136191";

            for (int i = 0; i < 10; i++) {
                final int index = i;
                completionService.submit(new Callable<List<QuotaField>>() {
                    public List<QuotaField> call() throws Exception {

                        return jdbcTemplate.query(SQL.toLowerCase().replaceAll(tableName, tableName + "_" + index),
                                new ResultSetExtractor<List<QuotaField>>() {
                                    public List<QuotaField> extractData(ResultSet resultSet) throws SQLException,
                                            DataAccessException {

                                        List<QuotaField> quotaFields = new ArrayList<>();
                                        while (resultSet.next()) {
                                            Double value = resultSet.getDouble("pv");
                                            QuotaField quotaField = new QuotaField(quota, value);
                                            quotaField.setDimension(dimension);
                                            quotaFields.add(quotaField);
                                        }

                                        return quotaFields;
                                    }
                                });

                    }
                });
            }

            List<QuotaField> allQuotaFields = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                List<QuotaField> quotaFields = completionService.take().get();
                allQuotaFields.addAll(quotaFields);
            }

            logger.info("quota: {},size: {}", quota, allQuotaFields.size());
            return allQuotaFields;
        }

    }

    public List<List<QuotaField>> getExampleStatistics() {

        List<List<QuotaField>> quotaResults = new ArrayList<>();
        CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                executorService);

        completionService.submit(new QuotaCalculationTask("select count(*) pv from ca_summary_136191", Quota.PV,
                new Dimension()));
        completionService.submit(new QuotaCalculationTask("select count(*) pv from ca_summary_136191", Quota.UV,
                new Dimension()));
        completionService.submit(new QuotaCalculationTask("select count(*) pv from ca_summary_136191", Quota.VISITORS,
                new Dimension()));

        for (int i = 0; i < 3; i++) {

            try {

                List<QuotaField> quotaFields = completionService.take().get();
                quotaResults.add(quotaFields);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();

        return quotaResults;
    }
}
