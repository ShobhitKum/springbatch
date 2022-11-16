package com.demo.batchprocessing.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

@Data
public class Sales implements Serializable {

    private long id;
    private Date order_date;
    private int sales_category;
    private String store_id;
    private float order_value;
    private String cat;

    public Sales(long id, Date order_date, int sales_category, String store_id, float order_value, String cat) {
        this.id = id;
        this.order_date = order_date;
        this.sales_category = sales_category;
        this.store_id = store_id;
        this.order_value = order_value;
        this.cat=cat;
    }

    @Override
    public String toString() {
        return "Sales{" +
                "id=" + id +
                ", order_date=" + order_date +
                ", sales_category=" + sales_category +
                ", store_id='" + store_id + '\'' +
                ", order_value=" + order_value +
                ", cat=" + cat +
                '}';
    }
}