package com.aytmatech.querier.model;

import com.aytmatech.querier.annotation.Column;
import com.aytmatech.querier.annotation.Table;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Test entity for orders. */
@Table("orders")
public class Order {
  private Long id;
  private Long customerId;
  private BigDecimal total;
  private OrderStatus status;
  private LocalDateTime createdAt;
  private Long productId;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  @Column("customer_id")
  public Long getCustomerId() {
    return customerId;
  }

  public void setCustomerId(Long customerId) {
    this.customerId = customerId;
  }

  public BigDecimal getTotal() {
    return total;
  }

  public void setTotal(BigDecimal total) {
    this.total = total;
  }

  public OrderStatus getStatus() {
    return status;
  }

  public void setStatus(OrderStatus status) {
    this.status = status;
  }

  @Column("created_at")
  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  @Column("product_id")
  public Long getProductId() {
    return productId;
  }

  public void setProductId(Long productId) {
    this.productId = productId;
  }
}
