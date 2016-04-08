package com.cubead.test.base;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.performance.martix.MatrixTableSearch;
import com.cubead.performance.martix.MatrixTableSearch.QuotaField;

public class ProductJdbcTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MatrixTableSearch matrixTableSearch;

    @Test
    public void testMatrixTableSearch() {
        long t1 = System.currentTimeMillis();
        Assert.assertNotNull(matrixTableSearch);
        List<List<QuotaField>> result = matrixTableSearch.getExampleStatistics();

        logger.info("Query Cost:{}", (System.currentTimeMillis() - t1) + " ms");

        logger.info("查询结果：{}", result.size());
        for (List<QuotaField> quotaFields : result) {
            logger.info("查询结果：{}", quotaFields.size());
        }
        logger.info("Query Cost:{}", (System.currentTimeMillis() - t1) + " ms");
    }
}
