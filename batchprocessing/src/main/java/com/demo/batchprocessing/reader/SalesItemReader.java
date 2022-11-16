package com.demo.batchprocessing.reader;

import com.demo.batchprocessing.bean.Sales;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class SalesItemReader implements ItemReader<Sales> {
    @Override
    public Sales read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return null;
    }
}
