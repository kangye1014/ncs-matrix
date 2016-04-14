package com.cubead.performance.martix;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

@Component
public class QuatoSplitCalculationExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MatrixTableSearch.class);

    private static ExecutorService executorService = new ThreadPoolExecutor(10, 30, 10, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>());

    @Autowired
    JdbcTemplate jdbcTemplate;

    public RowMergeResultSet calculatAllMergeResultSet(QueryUnit... quotaunits) {

        if (quotaunits == null)
            return null;

        RowMergeResultSet rowMergeResultSet = new RowMergeResultSet();
        final CountDownLatch latch = new CountDownLatch(quotaunits.length);

        for (QueryUnit queryUnit : quotaunits) {
            executorService.execute(new CalculatSqlRowTask(queryUnit, rowMergeResultSet, latch));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return rowMergeResultSet;

    }

    public static class QueryUnit {

        private String sql;
        private Set<Quota> quotas;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Set<Quota> getQuotas() {
            return quotas;
        }

        public void setQuotas(Set<Quota> quotas) {
            this.quotas = quotas;
        }
    }

    class CalculatSqlRowTask implements Runnable {

        private QueryUnit queryUnit;
        private RowMergeResultSet rowMergeResultSet;
        private CountDownLatch latch;
        private SqlDismantling sqlDismantling;

        public CalculatSqlRowTask(QueryUnit queryUnit, RowMergeResultSet rowMergeResultSet, CountDownLatch latch) {
            super();
            this.queryUnit = queryUnit;
            this.rowMergeResultSet = rowMergeResultSet;
            this.latch = latch;
            this.sqlDismantling = new SqlDismantling(queryUnit);
        }

        @Override
        public void run() {
            final Dimension dimension = new Dimension(sqlDismantling.getFields());
            jdbcTemplate.query(queryUnit.sql, new ResultSetExtractor<Object>() {
                public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                    while (resultSet.next()) {
                        SQLRowResultMapping sqlRowResultMapping = new SQLRowResultMapping(dimension);
                        List<QuotaWithValue> quotaWithValues = new ArrayList<>();
                        for (Quota quota : sqlDismantling.getQuotas()) {
                            QuotaWithValue quotaWithValue = new QuotaWithValue(quota);
                            quotaWithValue.setValue(resultSet.getDouble(quota.getQuota()));
                            quotaWithValues.add(quotaWithValue);
                        }
                        sqlRowResultMapping.setQuotaWithValues(quotaWithValues);
                        rowMergeResultSet.addRowMergeResult(sqlRowResultMapping);
                    }
                    latch.countDown();
                    return null;
                }
            });
        }
    }

    static class SqlDismantling {

        private QueryUnit queryUnit;
        private Set<String> allFields;
        private Set<Quota> quotas;

        public SqlDismantling(QueryUnit queryUnit) {
            this.queryUnit = queryUnit;
            validateQuotaSql();
        }

        public Set<String> getFields() {
            return allFields;
        }

        public Set<Quota> getQuotas() {
            return quotas;
        }

        public void validateQuotaSql() {

            if (queryUnit == null || queryUnit.sql == null || queryUnit.quotas == null)
                throw new IllegalArgumentException("queryUnit信息不完整,存在空值");

            String lowSql = queryUnit.sql.toLowerCase();
            int startIndex = lowSql.indexOf("select") + 6;
            int endIndex = lowSql.indexOf("from");

            Set<Quota> quotasInUnit = queryUnit.quotas;
            String[] fieldSet = lowSql.substring(startIndex, endIndex).split(",");

            allFields = new HashSet<String>();
            quotas = new HashSet<>();

            for (String field : fieldSet) {

                field = field.trim();

                int asIndex = field.indexOf(" as ");
                if (asIndex > -1)
                    field = field.substring(asIndex + 3);

                int emptyIndex = field.indexOf(" ");
                if (emptyIndex > -1)
                    field = field.substring(emptyIndex + 1).trim();

                Quota quota = Quota.getByQuota(field);
                if (quota == null || !quotasInUnit.contains(quota)) {
                    allFields.add(field);
                } else {
                    quotas.add(quota);
                }
            }

            if (quotas.size() < quotasInUnit.size()) {
                // throw new IllegalArgumentException("queryUnit中指标不完全存在于语句中");
            }

            if (allFields.size() == 0) {
                // throw new IllegalArgumentException("queryUnit不存在任何维度");
            }

        }
    }

    public static void main(String[] args) {

        QueryUnit queryUnit = new QueryUnit();
        queryUnit.setSql(" select ssElect a, b , c as c1 , fromov ,pv, cost, dfs as e fRom jkljlkj");

        Set<Quota> quotas = new HashSet<Quota>();
        quotas.add(Quota.COST);
        quotas.add(Quota.PV);
        queryUnit.setQuotas(quotas);

        System.out.println(quotas);

        SqlDismantling sqlDismantling = new SqlDismantling(queryUnit);

        System.out.println(sqlDismantling.getFields());
        System.out.println(sqlDismantling.getQuotas());

    }
}
