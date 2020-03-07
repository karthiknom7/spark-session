package com.sparkTutorial.sparksql.dao;

import java.io.Serializable;

public class ForecastNumber implements Serializable {
    private String divisionCode;
    private String categoryCode;
    private String bucket;
    private Integer year;
    private Integer period;
    private Integer week;
    private Double value;


    public ForecastNumber(String divisionCode, String categoryCode, Integer year, Integer period, Integer week, Double value) {
        this.divisionCode = divisionCode;
        this.categoryCode = categoryCode;
        this.year = year;
        this.period = period;
        this.week = week;
        this.value = value;
    }

    public ForecastNumber() {
    }

    public String getDivisionCode() {
        return divisionCode;
    }

    public void setDivisionCode(String divisionCode) {
        this.divisionCode = divisionCode;
    }

    public String getCategoryCode() {
        return categoryCode;
    }

    public void setCategoryCode(String categoryCode) {
        this.categoryCode = categoryCode;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getPeriod() {
        return period;
    }

    public void setPeriod(Integer period) {
        this.period = period;
    }

    public Integer getWeek() {
        return week;
    }

    public void setWeek(Integer week) {
        this.week = week;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
