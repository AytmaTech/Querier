package com.aytmatech.querier.model;

import com.aytmatech.querier.annotation.Table;
import java.math.BigDecimal;

/** Test entity for orders with catalog and schema qualifiers. */
@Table(value = "orders", schema = "sales", catalog = "mydb")
public class CatalogSchemaOrder {
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
