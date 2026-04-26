package com.aytmatech.querier.model;

import com.aytmatech.querier.annotation.Table;
import java.math.BigDecimal;

/** Test entity for products. */
@Table("products")
public class Product {
  private Long id;
  private String name;
  private BigDecimal price;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }
}
