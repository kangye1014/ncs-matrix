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

    abstract class QuotaCalculationTask implements Callable<List<QuotaField>> {

        private String SQL;
        private Quota quota;
        @SuppressWarnings("unused")
        private Dimension dimension;

        abstract Double resultMapping(ResultSet resultSet) throws SQLException;

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

            final String[] tableNames = { "ca_summary_136191_uv", "ca_summary_136191_impressions",
                    "ca_summary_136191_pv", "ca_summary_136191_cost" };

            for (int i = 0; i < 10; i++) {
                final int index = i;
                completionService.submit(new Callable<List<QuotaField>>() {
                    public List<QuotaField> call() throws Exception {

                        String activSql = SQL;
                        for (final String tableName : tableNames) {
                            activSql = activSql.toLowerCase().replaceAll(tableName, tableName + "_" + index);
                        }
                        // logger.info(activSql);
                        return jdbcTemplate.query(activSql, new ResultSetExtractor<List<QuotaField>>() {
                            public List<QuotaField> extractData(ResultSet resultSet) throws SQLException,
                                    DataAccessException {

                                List<QuotaField> quotaFields = new ArrayList<>();
                                while (resultSet.next()) {
                                    Double value = resultMapping(resultSet);
                                    QuotaField quotaField = new QuotaField(quota, value);
                                    quotaField.setDimension(new Dimension());
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
                // logger.info("子表查询结果：{}", quotaFields);
                allQuotaFields.addAll(quotaFields);
            }

            // logger.info("quota: {},size: {}", quota, allQuotaFields.size());
            return allQuotaFields;
        }
    }

    public List<List<QuotaField>> getExampleStatistics() {

        List<List<QuotaField>> quotaResults = new ArrayList<>();
        CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                executorService);

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });

        // 计算维度 cost
        completionService.submit(new QuotaCalculationTask(COST_SQL, Quota.PV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("cost");
            }
        });
        // 计算维度impression
        completionService.submit(new QuotaCalculationTask(IMPRESSIONS_SQL, Quota.UV, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("impressions");
            }
        });
        // 计算维度pv
        completionService.submit(new QuotaCalculationTask(PV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("pv");
            }
        });
        // 计算维度uv
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        // 计算维度
        completionService.submit(new QuotaCalculationTask(UV_SQL, Quota.VISITORS, new Dimension()) {
            Double resultMapping(ResultSet resultSet) throws SQLException {
                return resultSet.getDouble("uv");
            }
        });
        for (int i = 0; i < 40; i++) {

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

    /* 多日各关键词消费合计 */
    public static final String COST_SQL = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(cost) cost FROM ca_summary_136191_cost "
            + "WHERE log_day >=23 AND log_day <= 50 GROUP BY sub_tenant_id, campaign, adgroup, keyword";

    /* 多日各关键词展示量合计 */
    public static final String IMPRESSIONS_SQL = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(impressions) impressions FROM ca_summary_136191_impressions"
            + " WHERE log_day >=23 AND log_day <= 50 GROUP BY sub_tenant_id, campaign, adgroup, keyword";

    /* 每日各关键词合计pv */
    public static final String PV_SQL = "SELECT sub_tenant_id, campaign, adgroup, keyword, COUNT(*) pv  FROM ca_summary_136191_pv "
            + "WHERE log_day >=23 AND log_day <= 50 GROUP BY sub_tenant_id, campaign, adgroup, keyword";

    /* 每日各关键词合计uv */
    public static final String UV_SQL = " SELECT sub_tenant_id, campaign, adgroup, keyword, COUNT(distinct own_uid) as uv "
            + "FROM ca_summary_136191_uv WHERE log_day >=23 AND log_day <= 50 GROUP BY sub_tenant_id, campaign, adgroup, keyword";

}
