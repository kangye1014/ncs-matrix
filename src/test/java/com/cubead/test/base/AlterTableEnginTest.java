package com.cubead.test.base;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cubead.test.base.SqlRandomGenerator.TableEngine;

public class AlterTableEnginTest extends BaseTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // @Test
    public void updateTableInnoDBEngine() {
        for (String alterSql : SqlRandomGenerator.updateEnginesSql(TableEngine.InnoDB)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }

    // @Test
    public void updateTableMyISAMEngine() {
        for (String alterSql : SqlRandomGenerator.updateEnginesSql(TableEngine.MyISAM)) {
            jdbcTemplate.execute(alterSql);
            logger.info("{}更新成功!", alterSql);
        }
    }
}
