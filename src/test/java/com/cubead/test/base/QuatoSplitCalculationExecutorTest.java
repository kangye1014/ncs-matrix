package com.cubead.test.base;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.cubead.performance.martix.QuatoSplitCalculationExecutor;
import com.cubead.performance.martix.QuatoSplitCalculationExecutor.QueryUnit;
import com.cubead.performance.martix.Quota;
import com.cubead.performance.martix.RowMergeResultSet;

public class QuatoSplitCalculationExecutorTest extends BaseTest {

    @Autowired
    private QuatoSplitCalculationExecutor quatoSplitCalculationExecutor;

    private static QueryUnit roiQueryUnit;
    private static QueryUnit compressedQueryUnit;
    private static QueryUnit pvQueryUnit;

    @BeforeClass
    public static void initQueryUnit() {

        // roi init
        roiQueryUnit = new QueryUnit();
        roiQueryUnit.setSql(new StringBuilder()
                .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(costs_per_click) roi ")
                .append(" from ca_summary_136191_roi ").append(" where log_day >= 6 AND log_day <= 55 ")
                .append(" GROUP BY sub_tenant_id, campaign, adgroup, keyword  order by roi").toString());
        roiQueryUnit.setQuotas(Quota.ROI);

        // compressed
        compressedQueryUnit = new QueryUnit();
        compressedQueryUnit
                .setSql(new StringBuilder()
                        .append("SELECT sub_tenant_id, campaign, adgroup, keyword, sum(ext_resource_count) ext_resource_count, sum(impressions) impressions ")
                        .append(" from ca_summary_136191_compressed ").append(" where log_day >= 6 AND log_day <= 55 ")
                        .append(" GROUP BY sub_tenant_id, campaign, adgroup, keyword ").toString());
        compressedQueryUnit.setQuotas(Quota.IMPRESSION, Quota.EXT_RESOURCE_COUNT);

        // pv
        pvQueryUnit = new QueryUnit();
        pvQueryUnit.setSql(new StringBuilder().append("SELECT sub_tenant_id, campaign, adgroup, keyword, count(*) pv ")
                .append(" from ca_summary_136191_compressed ").append(" where log_day >= 6 AND log_day <= 55 ")
                .append(" GROUP BY sub_tenant_id, campaign, adgroup, keyword ").toString());
        pvQueryUnit.setQuotas(Quota.PV);

    }

    @Test
    public void calculatAllMergeResultSetTest() {

        Assert.assertNotNull(quatoSplitCalculationExecutor);
        RowMergeResultSet rowMergeResultSet = quatoSplitCalculationExecutor.calculatAllMergeResultSet(roiQueryUnit,
                compressedQueryUnit, pvQueryUnit);

        logger.info("查询结果合集:{}", rowMergeResultSet.getRowQuotaSetMap().size());
    }
}
