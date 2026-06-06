package com.aytmatech.querier.model;

import jakarta.persistence.Column;
import jakarta.persistence.Table;
import java.math.BigDecimal;

/** Test entity using JPA annotations instead of custom ones. */
@Table(name = "jpa_products")
public class JpaProduct {
  private Long id;
  private String productName;
  private BigDecimal price;
  private String description;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  @Column(name = "product_name")
  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
