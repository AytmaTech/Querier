package com.aytmatech.querier.model;

import com.aytmatech.querier.annotation.Table;
import java.math.BigDecimal;

/** Test entity for orders with schema qualifier. */
@Table(value = "orders", schema = "sales")
public class SchemaOrder {
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
