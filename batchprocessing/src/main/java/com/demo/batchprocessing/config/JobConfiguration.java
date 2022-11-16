package com.demo.batchprocessing.config;

/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.demo.batchprocessing.bean.Sales;
import com.demo.batchprocessing.mapper.SalesRowMapper;


import com.demo.batchprocessing.partitioner.ColumnRangePartitioner;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

/**
 * @author Shobhit
 */
@Configuration
public class JobConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;


    @Bean
    public ColumnRangePartitioner partitioner()
    {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
        columnRangePartitioner.setColumn("id");
        columnRangePartitioner.setDataSource(dataSource);
        columnRangePartitioner.setTable("sales");
        return columnRangePartitioner;
    }

 //reader
    @Bean
    @StepScope
    public JdbcPagingItemReader<Sales> pagingItemReader(
            @Value("#{stepExecutionContext['minValue']}") Long minValue,
            @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        JdbcPagingItemReader<Sales> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(this.dataSource);
        reader.setFetchSize(10);
        reader.setRowMapper(new SalesRowMapper());

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("sales.id, sales.order_date, sales.sales_category,sales_cat.cat, sales.store_id,sales.order_value");
        queryProvider.setFromClause("from sales left join sales_cat on sales.sales_category = sales_cat.cat_id");
       // queryProvider.setWhereClause("processed is not null");
        queryProvider.setWhereClause("where sales.id >= " + minValue + " and sales.id < " + maxValue);


        Map<String, Order> sortKeys = new HashMap<>(1);

        sortKeys.put("sales.id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);

        return reader;
    }

    //writer
    @Bean
    public ItemWriter<Sales> salesItemWriter() {
        System.out.println("Calling writer=================>");
        JdbcBatchItemWriter<Sales> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("INSERT INTO SALES_SUMMARY VALUES (null, :cat, :order_value, :order_date, :store_id)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        itemWriter.afterPropertiesSet();

        return itemWriter;

//        return items -> {
//            for (Sales item : items) {
//                System.out.println(item.toString());
//            }
//        };
    }


    //slave
    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("step1")
                .<Sales, Sales>chunk(10)
                .reader(pagingItemReader(null,null))
                .writer(salesItemWriter()).taskExecutor(taskExecutor())
                .build();
    }

    // Master
    @Bean
    public Step step1()
    {
        return stepBuilderFactory.get("step1")
                .partitioner(slaveStep().getName(), partitioner())
                .step(slaveStep())
                .gridSize(4)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("concurrentTask-query")
                .start(step1())
                .build();
    }


    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10); //threads in parallel
        return asyncTaskExecutor;
    }
}
