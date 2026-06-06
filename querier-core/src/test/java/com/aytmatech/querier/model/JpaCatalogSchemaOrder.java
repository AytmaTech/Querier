package com.aytmatech.querier.model;

import jakarta.persistence.Table;
import java.math.BigDecimal;

/** Test entity using JPA annotation with catalog and schema. */
@Table(name = "orders", schema = "reporting", catalog = "analytics_db")
public class JpaCatalogSchemaOrder {
  private Long id;
  private BigDecimal total;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public BigDecimal getTotal() {
    return total;
  }

  public void setTotal(BigDecimal total) {
    this.total = total;
  }
}
