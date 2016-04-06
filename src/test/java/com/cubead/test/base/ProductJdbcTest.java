package com.cubead.test.base;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.performance.martix.MatrixTableSearch;

public class ProductJdbcTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MatrixTableSearch matrixTableSearch;

    @Test
    public void testJdbcIsAutowired() {

    }

    @Test
    public void testMatrixTableSearch() {
        Assert.assertNotNull(matrixTableSearch);
        matrixTableSearch.getExampleStatistics();
    }
}
