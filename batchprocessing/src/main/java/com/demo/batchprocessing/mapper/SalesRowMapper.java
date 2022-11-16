package com.demo.batchprocessing.mapper;

import com.demo.batchprocessing.bean.Sales;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SalesRowMapper implements RowMapper<Sales> {
    @Override
    public Sales mapRow(ResultSet resultSet, int i) throws SQLException {
        return new Sales(resultSet.getLong("id"),
                resultSet.getDate("order_date"),
                resultSet.getInt("sales_category"),
                resultSet.getString("store_id"),
                resultSet.getFloat("order_value"),
                resultSet.getString("cat")
                );
    }
}


