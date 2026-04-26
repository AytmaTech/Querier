# Querier

A **type-safe Java SQL query builder library** that helps you construct SQL queries using the Builder pattern with method references for compile-time safety.

## Features

- ✅ **Type-safe**: Use Java method references (e.g., `Order::getId`) to reference columns, avoiding raw strings
- ✅ **Readable and maintainable**: Builder pattern makes queries easy to read and modify
- ✅ **Reporting/analytics friendly**: Support complex queries with aggregations, GROUP BY, HAVING, subqueries, window functions, CTEs, UNION/INTERSECT/EXCEPT
- ✅ **Execution-agnostic**: Built-in support for named (`:param`), positional (`?`), and indexed (`$1`) parameter styles — works with Spring JDBC, plain JDBC, jOOQ, Hibernate, R2DBC, Vert.x, and more
- ✅ **JPA Compatible**: Supports both custom annotations and standard JPA annotations (`jakarta.persistence` or `javax.persistence`)
- ✅ **Zero runtime dependencies**: Lightweight library with no external dependencies (JPA is optional)
- ✅ **Java 17+**: Built with modern Java features

## Requirements

- **Java 17** or higher
- **Maven 3.6+** (for building from source)

## Installation

### Maven

Add the following to your `pom.xml`:

```xml
<dependency>
    <groupId>com.aytmatech</groupId>
    <artifactId>querier</artifactId>
    <version>${querier.version}</version>
</dependency>
```

### Gradle

```gradle
implementation 'com.aytmatech:querier:$querierVersion'
```

## Quick Start

### 1. Define Entity Classes

Annotate your entity classes with `@Table` and `@Column`. You can use either custom annotations or JPA annotations:

**Option 1: Custom Annotations**

```java
import com.aytmatech.querier.annotation.Table;
import com.aytmatech.querier.annotation.Column;

@Table("orders")
public class Order {
    private Long id;
    private Long customerId;
    private BigDecimal total;
    private OrderStatus status;

    public Long getId() {
        return id;
    }

    @Column("customer_id")
    public Long getCustomerId() {
        return customerId;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public OrderStatus getStatus() {
        return status;
    }
}
```

**Option 2: JPA Annotations (Jakarta or javax.persistence)**
```java
import jakarta.persistence.Table;
import jakarta.persistence.Column;

@Table(name = "orders")
public class Order {
    private Long id;
    private Long customerId;
    private BigDecimal total;
    private OrderStatus status;
    
    public Long getId() { return id; }
    
    @Column(name = "customer_id")
    public Long getCustomerId() { return customerId; }
    
    public BigDecimal getTotal() { return total; }
    public OrderStatus getStatus() { return status; }
}
```

**Note**: Custom annotations take precedence if both are present. If no annotations are used, the library defaults to snake_case conversion.

### 2. Build a Query

```java
import com.aytmatech.querier.Condition;
import com.aytmatech.querier.Select;

Select select = Select.builder()
        .select(Order::getId).select(Order::getTotal)
        .from(Order.class)
        .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
        .build();

Select.SqlAndParams sp = select.toSqlAndParams();
// sp.sql() -> "SELECT orders.id, orders.total FROM orders WHERE orders.status = :param0"
// sp.params() -> {param0: "PAID"}
```

### 3. Execute the Query

Using Spring's `EntityManager`:

```java
EntityManager em = ...;
Select.SqlAndParams sp = select.toSqlAndParams();
Query nativeQuery = em.createNativeQuery(sp.sql(), Tuple.class);
sp.params().forEach(nativeQuery::setParameter);
List<?> rows = nativeQuery.getResultList();
```

## Framework Compatibility

Querier supports multiple SQL output formats to work seamlessly with different Java database frameworks and APIs.

### Compatibility Matrix

| Framework / API | Method | Placeholder Style | Output |
|---|---|---|---|
| **Spring `NamedParameterJdbcTemplate`** | `toSqlAndParams()` | `:paramName` | `SqlAndParams(sql, Map)` |
| **Spring `JdbcTemplate`** | `toPositionalSql()` | `?` | `PositionalSqlAndParams(sql, List)` |
| **Plain JDBC `PreparedStatement`** | `toPositionalSql()` | `?` | `PositionalSqlAndParams(sql, List)` |
| **Hibernate native query** | `toSqlAndParams()` | `:paramName` | `SqlAndParams(sql, Map)` |
| **jOOQ** | `toPositionalSql()` | `?` | `PositionalSqlAndParams(sql, List)` |
| **R2DBC `DatabaseClient`** | `toSqlAndParams()` | `:paramName` | `SqlAndParams(sql, Map)` |
| **Vert.x SQL Client** | `toIndexedSql()` | `$1, $2, ...` | `IndexedSqlAndParams(sql, List)` |
| **Logging / Debugging** | `toPlainSql()` | Values inlined | `String` |

### Usage Examples by Framework

#### Spring NamedParameterJdbcTemplate

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.SqlAndParams sp = select.toSqlAndParams();
List<Map<String,Object>> rows = namedJdbc.queryForList(sp.sql(), sp.params());
```

#### Spring JdbcTemplate

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.PositionalSqlAndParams sp = select.toPositionalSql();
List<Map<String,Object>> rows = jdbc.queryForList(sp.sql(), sp.params().toArray());
```

#### Plain JDBC PreparedStatement

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.PositionalSqlAndParams sp = select.toPositionalSql();
PreparedStatement ps = connection.prepareStatement(sp.sql());
for (int i = 0; i < sp.params().size(); i++) {
    ps.setObject(i + 1, sp.params().get(i));
}
ResultSet rs = ps.executeQuery();
```

#### jOOQ

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.PositionalSqlAndParams sp = select.toPositionalSql();
Result<Record> result = dsl.resultQuery(sp.sql(), sp.params().toArray()).fetch();
```

#### Hibernate Native Query

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.SqlAndParams sp = select.toSqlAndParams();
Query query = session.createNativeQuery(sp.sql(), Tuple.class);
sp.params().forEach(query::setParameter);
List<?> results = query.getResultList();
```

#### R2DBC DatabaseClient

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.SqlAndParams sp = select.toSqlAndParams();
DatabaseClient.GenericExecuteSpec spec = client.sql(sp.sql());
for (var entry : sp.params().entrySet()) {
    spec = spec.bind(entry.getKey(), entry.getValue());
}
Flux<Map<String,Object>> rows = spec.fetch().all();
```

#### Vert.x SQL Client

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

Select.IndexedSqlAndParams sp = select.toIndexedSql();
Tuple tuple = Tuple.tuple();
sp.params().forEach(tuple::addValue);
client.preparedQuery(sp.sql()).execute(tuple);
```

#### Logging / Debugging

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();

String plainSql = select.toPlainSql();
log.debug("Executing query: {}", plainSql);
// WARNING: Not safe for execution - use for logging only
```

## Identifier Quoting

Querier supports database-specific identifier quoting to handle reserved words and special characters in table and column names (e.g., `order`, `user`, `group`, `select`, `table`).

### QuoteStrategy Options

Different databases use different quoting mechanisms:

| Database | QuoteStrategy | Example Output |
|---|---|---|
| **Default (no quoting)** | `NONE` | `orders.id` |
| **PostgreSQL, Oracle, SQLite** | `ANSI` | `"orders"."id"` |
| **MySQL, MariaDB** | `MYSQL` | `` `orders`.`id` `` |
| **SQL Server, Azure SQL** | `SQL_SERVER` | `[orders].[id]` |

### Using QuoteStrategy

Set the quote strategy when building your query:

```java
// PostgreSQL / Oracle / ANSI SQL
Select select = Select.builder()
    .select(Order::getId)
    .from(Order.class)
    .quoteStrategy(QuoteStrategy.ANSI)
    .build();
// SQL: SELECT "orders"."id" FROM "orders"

// MySQL / MariaDB
Select select = Select.builder()
    .select(Order::getId)
    .from(Order.class)
    .quoteStrategy(QuoteStrategy.MYSQL)
    .build();
// SQL: SELECT `orders`.`id` FROM `orders`

// SQL Server / Azure SQL
Select select = Select.builder()
    .select(Order::getId)
    .from(Order.class)
    .quoteStrategy(QuoteStrategy.SQL_SERVER)
    .build();
// SQL: SELECT [orders].[id] FROM [orders]
```

### Default Behavior

The default `QuoteStrategy` is `NONE` for full backward compatibility. This means identifiers are not quoted unless explicitly specified.

### Quoting Coverage

The `QuoteStrategy` applies quoting to:
- Table names (including catalog and schema qualifiers)
- Column names in SELECT, WHERE, JOIN, GROUP BY, ORDER BY, and HAVING clauses
- JPA-annotated tables with `catalog` and `schema` attributes

**Example with schema-qualified names:**

```java
@Table(name = "orders", schema = "sales", catalog = "mydb")
public class Order { ... }

Select select = Select.builder()
    .select(Order::getId)
    .from(Order.class)
    .quoteStrategy(QuoteStrategy.ANSI)
    .build();
// SQL: SELECT "mydb"."sales"."orders"."id" FROM "mydb"."sales"."orders"
```

### When to Use Quoting

Use identifier quoting when:
- Your table or column names are SQL reserved words (`order`, `user`, `group`, `select`, etc.)
- Your identifiers contain special characters or spaces
- You want to preserve case sensitivity (e.g., `MyTable` vs `mytable`)
- Your database schema requires quoted identifiers

**Note**: Raw expressions and CTEs are not automatically quoted as they may contain complex SQL. Quote those manually if needed.

## Usage Examples

### Basic SELECT

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .build();
// SQL: SELECT orders.id, orders.total FROM orders
```

### WHERE Conditions

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
    .build();
// SQL: SELECT orders.id, orders.total FROM orders WHERE orders.status = :param0
```

#### Available Condition Types

- **Comparisons**: `eq()`, `ne()`, `gt()`, `lt()`, `gte()`, `lte()`
- **Pattern matching**: `like()`
- **Range**: `between()`, `in()`
- **NULL checks**: `isNull()`, `isNotNull()`
- **Logical operators**: `and()`, `or()`, `not()`
- **Subqueries**: `exists()`

```java
// Complex conditions
Select select = Select.builder()
    .select(Order::getId)
    .from(Order.class)
    .where(Condition.and(
        Condition.or(
            Condition.eq(Order::getStatus, OrderStatus.PAID),
            Condition.eq(Order::getStatus, OrderStatus.SHIPPED)
        ),
        Condition.gt(Order::getTotal, new BigDecimal("50"))
    ))
    .build();
```

### JOINs

```java
Select select = Select.builder()
    .select(Order::getId).select(Customer::getName)
    .from(Order.class)
    .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
    .build();
// SQL: SELECT orders.id, customers.name FROM orders INNER JOIN customers ON orders.customer_id = customers.id
```

#### Available JOIN Types

- `join()` - INNER JOIN
- `leftJoin()` - LEFT JOIN
- `rightJoin()` - RIGHT JOIN
- `fullJoin()` - FULL OUTER JOIN
- `crossJoin()` - CROSS JOIN

### Aggregations

```java
Select select = Select.builder()
    .select(Order::getStatus).select(Aggregate.sum(Order::getTotal).as("total_revenue"))
    .from(Order.class)
    .groupBy(Order::getStatus)
    .having(Condition.gt(Order::getTotal, new BigDecimal("1000")))
    .orderBy(OrderBy.desc("SUM(orders.total)"))
    .build();
// SQL: SELECT orders.status, SUM(orders.total) AS total_revenue FROM orders 
//      GROUP BY orders.status HAVING orders.total > :param0 
//      ORDER BY SUM(orders.total) DESC
```

#### Available Aggregate Functions

- `count()` - COUNT(column)
- `countAll()` - COUNT(*)
- `countDistinct()` - COUNT(DISTINCT column)
- `sum()` - SUM(column)
- `avg()` - AVG(column)
- `min()` - MIN(column)
- `max()` - MAX(column)

### Window Functions

```java
Select select = Select.builder()
    .select(Order::getId)
    .select(Order::getTotal)
    .select(WindowFunction.of(Aggregate.sum(Order::getTotal))
        .partitionBy(Order::getCustomerId)
        .orderBy(OrderBy.asc(Order::getCreatedAt))
        .as("running_total")
    )
    .from(Order.class)
    .build();
// SQL: SELECT orders.id, orders.total, 
//      SUM(orders.total) OVER (PARTITION BY orders.customer_id ORDER BY orders.created_at ASC) AS running_total
//      FROM orders
```

### Subqueries

```java
Select subquery = Select.builder()
    .select(Aggregate.avg(Order::getTotal).as("avg_total"))
    .from(Order.class)
    .build();

Select main = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.gt(Order::getTotal, subquery))
    .build();
// SQL: SELECT orders.id, orders.total FROM orders 
//      WHERE orders.total > (SELECT AVG(orders.total) AS avg_total FROM orders)
```

### CTEs (Common Table Expressions)

```java
Select cte = Select.builder()
    .select(Order::getCustomerId).select(Aggregate.sum(Order::getTotal).as("total_spent"))
    .from(Order.class)
    .groupBy(Order::getCustomerId)
    .build();

Select main = Select.builder()
    .with("customer_totals", cte)
    .select(Expression.raw("customer_totals.customer_id"))
    .select(Expression.raw("customer_totals.total_spent"))
    .from(Expression.tableRef("customer_totals"))
    .where(Condition.gt(Expression.raw("customer_totals.total_spent"), 5000))
    .build();
// SQL: WITH customer_totals AS (
//        SELECT orders.customer_id, SUM(orders.total) AS total_spent 
//        FROM orders GROUP BY orders.customer_id
//      )
//      SELECT customer_totals.customer_id, customer_totals.total_spent 
//      FROM customer_totals WHERE customer_totals.total_spent > :param0
```

### Set Operations

```java
Select select = Select.builder()
    .select(Order::getCustomerId)
    .from(Order.class)
    .build();

// UNION
Select unionSelect = Select.builder()
    .select(Order::getProductId)
    .from(Order.class)
    .union(select)
    .build();

// Also available: unionAll(), intersect(), except()
```

### ORDER BY, LIMIT, OFFSET

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .orderBy(OrderBy.desc(Order::getTotal), OrderBy.asc(Order::getId))
    .limit(10)
    .offset(20)
    .build();
// SQL: SELECT orders.id, orders.total FROM orders 
//      ORDER BY orders.total DESC, orders.id ASC LIMIT 10 OFFSET 20
```

### SELECT DISTINCT

```java
Select select = Select.builder()
    .distinct()
    .select(Order::getStatus)
    .from(Order.class)
    .build();
// SQL: SELECT DISTINCT orders.status FROM orders
```

## API Reference

### Core Classes

- **`Select`** - Main query builder with fluent API
- **`Select.SqlAndParams`** - Record containing SQL with named (`:param`) placeholders and parameter map
- **`Select.PositionalSqlAndParams`** - Record containing SQL with positional (`?`) placeholders and ordered parameter list
- **`Select.IndexedSqlAndParams`** - Record containing SQL with indexed (`$1, $2, ...`) placeholders and ordered parameter list
- **`Condition`** - WHERE and JOIN condition builder
- **`Aggregate`** - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- **`WindowFunction`** - Window functions with PARTITION BY and ORDER BY
- **`OrderBy`** - ORDER BY clause builder
- **`Expression`** - Raw SQL expressions
- **`CaseWhen`** - CASE WHEN expressions
- **`JoinType`** - Enum for JOIN types
- **`AggregateFunction`** - Enum for aggregate functions
- **`CaseKeyword`** - Enum for CASE WHEN expression keywords
- **`SetOperationType`** - Enum for set operation keywords
- **`SortDirection`** - Enum for sort direction for ORDER BY clauses
- **`SqlClause`** - Enum for clause keywords used in SELECT statements
- **`SqlOperator`** - Enum for comparison and logical operators
- **`SqlValue`** - Enum for special SQL values

### Annotations

Querier supports both custom annotations and standard JPA annotations:

**Custom Annotations** (from `com.aytmatech.querier.annotation`):
- **`@Table("table_name")`** - Specify custom table name
- **`@Column("column_name")`** - Specify custom column name

**JPA Annotations** (from `jakarta.persistence` or `javax.persistence`):
- **`@Table(name = "table_name")`** - Specify custom table name
- **`@Column(name = "column_name")`** - Specify custom column name

**Priority**: Custom annotations take precedence over JPA annotations if both are present.

### Naming Convention

By default, Querier converts camelCase getter names to snake_case column names:
- `getOrderTotal()` → `order_total`
- `getId()` → `id`
- `isActive()` → `active`

Override this with `@Column` or JPA `@Column` annotations when needed.

## Building from Source

```bash
# Clone the repository
git clone https://github.com/AytmaTech/querier.git
cd querier

# Build with Maven
mvn clean install

# Run tests
mvn test
```

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or suggestions, please open an issue on GitHub.

