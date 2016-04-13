package com.cubead.test.base;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.test.base.SqlRandomGenerator.TableEngine;

public class AlterTableEnginTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // @Test
    public void updateTableInnoDBEngine() {
        for (String alterSql : SqlRandomGenerator.updateEngines(TableEngine.InnoDB)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }

    @Test
    public void updateTableMyISAMEngine() {
        for (String alterSql : SqlRandomGenerator.updateEngines(TableEngine.MyISAM)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }
}