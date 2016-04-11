package com.cubead.test.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.cubead.performance.martix.MatrixTableSearch.Dimen;
import com.cubead.performance.martix.MatrixTableSearch.Dimension;
import com.cubead.performance.martix.MatrixTableSearch.Quota;
import com.cubead.performance.martix.MatrixTableSearch.QuotaField;

public class SqlTest extends BaseTest {

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    public static String select_sql = "SELECT sub_tenant_id, campaign, adgroup, keyword, sum(cost) cost ";
    public static String where_sql = "WHERE log_day >= 20 AND log_day <= 70 ";
    public static String group_sql = "GROUP BY adgroup,keyword,campaign,sub_tenant_id order by cost ";

    public static final String COST_SQL_1 = select_sql + "FROM ca_summary_136191_cost_1 " + where_sql + group_sql;
    public static final String COST_SQL_2 = select_sql + "FROM ca_summary_136191_cost_2 " + where_sql + group_sql;
    public static final String COST_SQL_3 = select_sql + "FROM ca_summary_136191_cost_3 " + where_sql + group_sql;
    public static final String COST_SQL_4 = select_sql + "FROM ca_summary_136191_cost_4 " + where_sql + group_sql;
    public static final String COST_SQL_5 = select_sql + "FROM ca_summary_136191_cost_5 " + where_sql + group_sql;
    public static final String COST_SQL_6 = select_sql + "FROM ca_summary_136191_cost_6 " + where_sql + group_sql;
    public static final String COST_SQL_7 = select_sql + "FROM ca_summary_136191_cost_7 " + where_sql + group_sql;
    public static final String COST_SQL_8 = select_sql + "FROM ca_summary_136191_cost_8 " + where_sql + group_sql;
    public static final String COST_SQL_9 = select_sql + "FROM ca_summary_136191_cost_9 " + where_sql + group_sql;
    public static final String COST_SQL_10 = select_sql + "FROM ca_summary_136191_cost_0 " + where_sql + group_sql;

    public final static String[] SQLS_STRIN = { COST_SQL_1, COST_SQL_2, COST_SQL_3, COST_SQL_4, COST_SQL_5, COST_SQL_6,
            COST_SQL_7, COST_SQL_8, COST_SQL_9, COST_SQL_10 };

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void sqlExecuteMutilThreadTimeTest() {

        long t1 = System.currentTimeMillis();
        CompletionService<List<QuotaField>> completionService = new ExecutorCompletionService<List<QuotaField>>(
                executorService);

        final Dimension dimension = new Dimension("sub_tenant_id", "campaign", "adgroup", "keyword");
        for (final String sql : SQLS_STRIN) {
            completionService.submit(new Callable<List<QuotaField>>() {
                public List<QuotaField> call() throws Exception {
                    return jdbcTemplate.query(sql, new ResultSetExtractor<List<QuotaField>>() {
                        public List<QuotaField> extractData(ResultSet resultSet) throws SQLException,
                                DataAccessException {
                            List<QuotaField> quotaFields = new ArrayList<>();
                            while (resultSet.next()) {

                                Dimension dimensionTemp = new Dimension();
                                List<Dimen> dimens = new ArrayList<>();
                                for (int i = 0; i < dimension.getDimens().size(); i++) {

                                    String field = dimension.getDimens().get(i).getField();
                                    Dimen dimen = new Dimen(field);
                                    dimen.setValue(resultSet.getObject(field));
                                    dimens.add(dimen);

                                }
                                dimensionTemp.setDimens(dimens);

                                QuotaField quotaField = new QuotaField(Quota.PV, resultSet.getDouble("cost"));
                                quotaField.setPaserKeys(dimensionTemp.parseAsKey());
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
            try {
                List<QuotaField> quotaFields = completionService.take().get();
                // logger.info("子表查询结果：{}", quotaFields);
                allQuotaFields.addAll(quotaFields);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("查询耗时:{}", (System.currentTimeMillis() - t1) + " ms");
    }

}
