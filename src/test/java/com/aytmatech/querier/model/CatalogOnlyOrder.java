package com.aytmatech.querier.model;

import com.aytmatech.querier.annotation.Table;
import java.math.BigDecimal;

/** Test entity for orders with catalog qualifier only (no schema). */
@Table(value = "orders", catalog = "mydb")
public class CatalogOnlyOrder {
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
