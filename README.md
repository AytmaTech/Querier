[![CI](https://github.com/AytmaTech/Querier/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AytmaTech/Querier/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.aytmatech/querier)](https://central.sonatype.com/artifact/com.aytmatech/querier)

# Querier

A **type-safe Java SQL query builder library** that helps you construct SQL queries using the Builder
pattern with method references for compile-time safety.

## Features

- ✅ **Type-safe**: Use Java method references (e.g., `Order::getId`) to reference columns, avoiding raw strings
- ✅ **Readable and maintainable**: Builder pattern makes queries easy to read and modify
- ✅ **Reporting/analytics friendly**: Support complex queries with aggregations, GROUP BY, HAVING, subqueries, window functions, CTEs, UNION/INTERSECT/EXCEPT
- ✅ **Execution-agnostic**: Built-in support for named (`:param`), positional (`?`), and indexed (`$1`) parameter styles — works with Spring JDBC, plain JDBC, jOOQ, Hibernate, R2DBC, Vert.x, and more
- ✅ **JPA Compatible**: Supports both custom annotations and standard JPA annotations (`jakarta.persistence` or `javax.persistence`)
- ✅ **Zero runtime dependencies**: Lightweight library with no external dependencies (JPA is optional)
- ✅ **Java 17+**: Built with modern Java features
- ✅ **Database Functions**: Type-safe SQL function calls for PostgreSQL, MySQL/MariaDB, SQL Server, and Oracle — via vendor dialect factories or fully custom resolvers
- ✅ **Server-side Dashboards** *(optional)*: Generate Chart.js-powered HTML dashboards directly from your Querier queries — no JavaScript required
- ✅ **AI-powered queries and dashboards** *(optional)*: Translate natural-language prompts into validated `Select` or `Dashboard` objects — bring your own key, works with any OpenAI-compatible endpoint (OpenAI, Azure OpenAI, Mistral, Ollama, etc.)

## Requirements

- **Java 17** or higher
- **Maven 3.8.1+** (for building from source)

---

## Modules

Querier is structured as a multi-module library published under a single version.
You only pull in what you need.

| Artifact | Description | Required |
|---|---|---|
| `querier-core` | Type-safe SQL query builder — all core features | ✅ Always |
| `querier-ui` | Server-side dashboard & Chart.js rendering | ➕ Optional |
| `querier-ai` | Natural-language → SQL → `Select` translation, AI-powered dashboard generation | ➕ Optional |

> **Note:** `querier-ui` declares `querier-core` as a transitive dependency.
> If you add `querier-ui`, you do **not** need to declare `querier-core` separately.

> **Note:** `querier-ai` declares `querier-core` as a required dependency and `querier-ui` as an optional one (needed only for AI dashboard generation). You do **not** need to declare them separately.
---

## Installation

Replace `QUERIER_VERSION` with the [latest release version](https://github.com/AytmaTech/querier/releases).

### Core only — SQL query building

#### Maven
```xml
<dependency>
    <groupId>com.aytmatech</groupId>
    <artifactId>querier-core</artifactId>
    <version>QUERIER_VERSION</version>
</dependency>
```

#### Gradle
```gradle
implementation 'com.aytmatech:querier-core:QUERIER_VERSION'
```

---

### Core + Dashboards UI *(optional)*

Add `querier-ui` when you also want server-side dashboard generation powered by Chart.js.
`querier-core` is included transitively — no need to declare both.

#### Maven
```xml
<dependency>
    <groupId>com.aytmatech</groupId>
    <artifactId>querier-ui</artifactId>
    <version>QUERIER_VERSION</version>
</dependency>
```

#### Gradle
```gradle
implementation 'com.aytmatech:querier-ui:QUERIER_VERSION'
```

### Core + AI *(optional)*

Add `querier-ai` when you want natural-language query generation or AI-powered dashboard creation.
`querier-core` is included transitively. Add `querier-ui` as well if you also need AI-generated dashboards.

#### Maven
```xml
<dependency>
    <groupId>com.aytmatech</groupId>
    <artifactId>querier-ai</artifactId>
    <version>QUERIER_VERSION</version>
</dependency>
```

#### Gradle
```gradle
implementation 'com.aytmatech:querier-ai:QUERIER_VERSION'
```

---

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

---

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

---

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

---

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

### Database Functions

`DbFunction` enables type-safe SQL function calls — vendor-specific or fully custom — usable in SELECT lists, WHERE conditions, ORDER BY clauses, and window function frames.

#### Using Vendor Dialect Factories

Each supported database has a dedicated factory class. Import the one that matches your target database:

| Factory class | Database |
|---|---|
| `PgDialectFunctions` | PostgreSQL |
| `MySqlDialectFunctions` | MySQL / MariaDB |
| `MsSqlDialectFunctions` | SQL Server / Azure SQL |
| `OracleDialectFunctions` | Oracle Database |

**PostgreSQL example — date truncation in SELECT:**
```java
import com.aytmatech.querier.vendor.PgDialectFunctions;

Select select = Select.builder()
    .select(PgDialectFunctions.dateTrunc("month", Order::getCreatedAt).as("month"))
    .select(Aggregate.sum(Order::getTotal).as("monthly_revenue"))
    .from(Order.class)
    .groupBy(Expression.raw("DATE_TRUNC('month', orders.created_at)"))
    .orderBy(OrderBy.asc("DATE_TRUNC('month', orders.created_at)"))
    .build();
// SQL: SELECT DATE_TRUNC('month', orders.created_at) AS month,
//            SUM(orders.total) AS monthly_revenue
//      FROM orders
//      GROUP BY DATE_TRUNC('month', orders.created_at)
//      ORDER BY DATE_TRUNC('month', orders.created_at) ASC
```

**MySQL example — null fallback and string formatting:**
```java
import com.aytmatech.querier.vendor.MySqlDialectFunctions;

Select select = Select.builder()
    .select(Order::getId)
    .select(MySqlDialectFunctions.ifNull(Order::getNotes, "N/A").as("notes"))
    .select(MySqlDialectFunctions.dateFormat(Order::getCreatedAt, "%Y-%m-%d").as("order_date"))
    .from(Order.class)
    .build();
// SQL: SELECT orders.id,
//            IFNULL(orders.notes, :param0) AS notes,
//            DATE_FORMAT(orders.created_at, :param1) AS order_date
//      FROM orders
```

**SQL Server example — conditional logic:**
```java
import com.aytmatech.querier.vendor.MsSqlDialectFunctions;

Select select = Select.builder()
    .select(Order::getId)
    .select(MsSqlDialectFunctions.isNull(Order::getNotes, "N/A").as("notes"))
    .select(MsSqlDialectFunctions.format(Order::getCreatedAt, "yyyy-MM").as("month"))
    .from(Order.class)
    .build();
// SQL: SELECT orders.id,
//            ISNULL(orders.notes, :param0) AS notes,
//            FORMAT(orders.created_at, :param1) AS month
//      FROM orders
```

**Oracle example — date handling:**
```java
import com.aytmatech.querier.vendor.OracleDialectFunctions;

Select select = Select.builder()
    .select(Order::getId)
    .select(OracleDialectFunctions.toChar(Order::getCreatedAt, "YYYY-MM").as("month"))
    .select(OracleDialectFunctions.nvl(Order::getNotes, "N/A").as("notes"))
    .from(Order.class)
    .build();
// SQL: SELECT orders.id,
//            TO_CHAR(orders.created_at, :param0) AS month,
//            NVL(orders.notes, :param1) AS notes
//      FROM orders
```

#### DbFunction in WHERE Conditions

```java
Select select = Select.builder()
    .select(Order::getId).select(Order::getTotal)
    .from(Order.class)
    .where(Condition.eq(PgDialectFunctions.dateTrunc("month", Order::getCreatedAt),
                        "2024-01-01"))
    .build();
// SQL: SELECT orders.id, orders.total FROM orders
//      WHERE DATE_TRUNC('month', orders.created_at) = :param0
```

#### DbFunction in ORDER BY

```java
Select select = Select.builder()
    .select(Order::getId)
    .from(Order.class)
    .orderBy(OrderBy.asc(PgDialectFunctions.lower(Order::getStatus)))
    .build();
// SQL: SELECT orders.id FROM orders ORDER BY LOWER(orders.status) ASC
```

#### DbFunction inside Window Functions

```java
Select select = Select.builder()
    .select(Order::getId)
    .select(WindowFunction.of(PgDialectFunctions.dateTrunc("month", Order::getCreatedAt))
        .partitionBy(Order::getCustomerId)
        .as("month_bucket"))
    .from(Order.class)
    .build();
```

#### Custom Functions with `DbFunction.of()`

For functions not covered by a dialect factory, use `DbFunction.of()` directly:

```java
// COALESCE with a column and a fallback value
DbFunction coalesce = DbFunction.of("COALESCE",
    DbFunction.col(Order::getNotes),
    DbFunction.val("N/A")
);
// SQL fragment: COALESCE(orders.notes, :param0)

// Function with a raw SQL suffix (e.g. STRING_AGG with ORDER BY)
DbFunction stringAgg = DbFunction.of("STRING_AGG",
    List.of(DbFunction.col(Order::getStatus), DbFunction.lit(", ")),
    " ORDER BY orders.status ASC"
);
// SQL fragment: STRING_AGG(orders.status, ', ' ORDER BY orders.status ASC)
```

Available `DbFunction.Arg` factory methods:

| Method | Rendered as | Use for |
|---|---|---|
| `DbFunction.col(Order::getField)` | `table.column` (quoted) | Type-safe column references |
| `DbFunction.val(value)` | `:paramN` | Dynamic bound values |
| `DbFunction.lit("text")` | `'text'` | Constant string arguments (date units, formats) |
| `DbFunction.nested(fn)` | inner function SQL | Composed / nested function calls |
| `DbFunction.raw("SQL")` | verbatim SQL | Keywords, type names, integer constants |

#### Fully Custom Functions with `DbFunction.custom()`

For non-standard syntax (e.g., `CAST(col AS type)`, `EXTRACT(field FROM col)`), implement the `DbFunction.Resolver` interface:

```java
DbFunction extractYear = DbFunction.custom((strategy, gen) -> {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(Order::getCreatedAt);
    String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
    return new DbFunction.Resolved("EXTRACT(YEAR FROM " + colSql + ")", Map.of());
});
// SQL fragment: EXTRACT(YEAR FROM orders.created_at)
```

#### Available Functions by Dialect

<details>
<summary><strong>PostgreSQL</strong> — <code>PgDialectFunctions</code></summary>

**Date / Time**
`dateTrunc(unit, col)`, `datePart(field, col)`, `extract(field, col)`, `toChar(col, format)`, `age(col)`, `age(end, start)`, `now()`

**String**
`upper(col)`, `lower(col)`, `initcap(col)`, `trim(col)`, `length(col)`, `left(col, n)`, `right(col, n)`, `lpad(col, length, fill)`, `rpad(col, length, fill)`, `concat(cols...)`, `concatWs(separator, cols...)`, `regexpReplace(col, pattern, replacement)`, `regexpReplace(col, pattern, replacement, flags)`, `stringAgg(col, separator)`, `stringAggOrdered(col, separator, orderCol, direction)`

**Array / JSON**
`arrayAgg(col)`, `arrayLength(col, dimension)`, `unnest(col)`, `jsonbExtractPathText(col, keys...)`, `jsonbExtractPath(col, keys...)`, `toJsonb(col)`

**Conditional / Null**
`coalesce(col, default)`, `coalesce(cols...)`, `nullif(col, value)`, `greatest(cols...)`, `least(cols...)`

**Math**
`round(col, places)`, `floor(col)`, `ceil(col)`, `abs(col)`, `sqrt(col)`, `power(col, exponent)`

**Type**
`cast(col, pgType)`

**Window**
`lag(col, offset, default)`, `lead(col, offset, default)`, `firstValue(col)`, `lastValue(col)`, `nthValue(col, n)`
</details>

<details>
<summary><strong>MySQL / MariaDB</strong> — <code>MySqlDialectFunctions</code></summary>

**Date / Time**
`dateFormat(col, format)`, `strToDate(col, format)`, `dateDiff(end, start)`, `dateAdd(col, amount, unit)`, `dateSub(col, amount, unit)`, `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `minute(col)`, `second(col)`, `week(col)`, `quarter(col)`, `lastDay(col)`

**String**
`upper(col)`, `lower(col)`, `trim(col)`, `length(col)`, `charLength(col)`, `left(col, n)`, `right(col, n)`, `substring(col, start, length)`, `lpad(col, length, fill)`, `rpad(col, length, fill)`, `concat(cols...)`, `concatWs(separator, cols...)`, `groupConcat(col)`, `groupConcat(col, separator)`, `instr(col, search)`, `findInSet(needle, col)`

**JSON**
`jsonExtract(col, paths...)`, `jsonValue(col, path)`, `jsonArrayAgg(col)`, `jsonObjectAgg(keyCol, valueCol)`

**Math**
`round(col, dp)`, `truncate(col, dp)`, `floor(col)`, `ceiling(col)`, `abs(col)`, `sqrt(col)`, `pow(col, exponent)`, `sign(col)`

**Conditional / Null**
`ifNull(col, default)`, `nullif(col, value)`, `coalesce(col, default)`

**Type**
`cast(col, mysqlType)`
</details>

<details>
<summary><strong>SQL Server / Azure SQL</strong> — <code>MsSqlDialectFunctions</code></summary>

**Date / Time**
`datePart(unit, col)`, `dateDiff(unit, start, end)`, `dateAdd(unit, amount, col)`, `format(col, format)`, `eoMonth(col)`

**String**
`upper(col)`, `lower(col)`, `trim(col)`, `len(col)`, `left(col, n)`, `right(col, n)`, `substring(col, start, length)`, `charIndex(search, col)`, `patIndex(pattern, col)`, `stuff(col, start, length, replacement)`, `concatWs(separator, cols...)`, `stringAgg(col, separator)`

**JSON**
`jsonValue(col, path)`, `jsonQuery(col, path)`

**Math**
`round(col, length)`, `ceiling(col)`, `floor(col)`, `abs(col)`, `sqrt(col)`, `power(col, exponent)`, `sign(col)`

**Conditional / Null**
`isNull(col, replacement)`, `nullif(col, value)`, `coalesce(col, default)`, `iif(col, trueResult, falseResult)`, `choose(col, options...)`

**Type**
`cast(col, sqlServerType)`, `tryCast(col, sqlServerType)`, `convert(sqlServerType, col, style)`

**Window**
`lag(col, offset, default)`, `lead(col, offset, default)`, `firstValue(col)`, `lastValue(col)`, `ntile(n)`
</details>

<details>
<summary><strong>Oracle Database</strong> — <code>OracleDialectFunctions</code></summary>

**Date / Time**
`trunc(col, format)`, `trunc(col, places)`, `addMonths(col, months)`, `monthsBetween(end, start)`, `lastDay(col)`, `nextDay(col, dayName)`, `extract(field, col)`, `toDate(col, format)`, `toTimestamp(col, format)`, `toChar(col, format)`, `sysdate()`, `systimestamp()`

**String**
`upper(col)`, `lower(col)`, `initcap(col)`, `trim(col)`, `ltrim(col)`, `rtrim(col)`, `length(col)`, `substr(col, start, length)`, `instr(col, search)`, `lpad(col, length, fill)`, `rpad(col, length, fill)`, `replace(col, search, replacement)`, `regexpReplace(col, pattern, replacement)`, `regexpSubstr(col, pattern)`, `concat(col1, col2)`, `listagg(col, separator, orderCol)`

**JSON**
`jsonValue(col, path)`, `jsonQuery(col, path)`

**Math**
`round(col, places)`, `ceil(col)`, `floor(col)`, `abs(col)`, `sqrt(col)`, `power(col, exponent)`, `sign(col)`, `mod(col, divisor)`

**Conditional / Null**
`nvl(col, default)`, `nvl2(col, notNullVal, nullVal)`, `coalesce(col, default)`, `nullif(col, value)`, `decode(col, when, then, ...)`

**Type**
`cast(col, oracleType)`, `toNumber(col)`, `toNumber(col, format)`

**Window**
`lag(col, offset, default)`, `lead(col, offset, default)`, `firstValue(col)`, `lastValue(col)`, `nthValue(col, n)`, `ntile(n)`
</details>
---

## Dashboards *(querier-ui)*

> **Requires** the `querier-ui` artifact. See [Installation](#installation).

Querier UI lets you build server-side HTML dashboards powered by [Chart.js](https://www.chartjs.org/)
directly from your Querier SQL queries — no JavaScript or frontend tooling required.

The rendered output is a **self-contained HTML page** (or an embeddable fragment) that you can serve
directly from any Java web framework.

### How it works

1. Define one or more **widgets**, each backed by a Querier `Select` query
2. Assemble them into a **`Dashboard`**
3. Provide a **`QueryRunner`** that bridges Querier to your database layer
4. Call **`renderHtml()`** or **`renderFragment()`** to get the output

### Supported Chart Types

`BAR` · `HORIZONTAL_BAR` · `LINE` · `PIE` · `DOUGHNUT` · `RADAR` · `POLAR_AREA` · `SCATTER` · `BUBBLE`

### Quick Start — Dashboards

```java
import com.aytmatech.querier.Aggregate;
import com.aytmatech.querier.Select;
import com.aytmatech.querier.dashboard.*;

// 1. Define a QueryRunner (shown here with Spring JdbcTemplate)
QueryRunner runner = select -> {
    Select.PositionalSqlAndParams sp = select.toPositionalSql();
    return jdbcTemplate.queryForList(sp.sql(), sp.params().toArray());
};

// 2. Define a widget
DashboardWidget revenueWidget = DashboardWidget.builder()
    .title("Revenue by Status")
    .chartType(ChartType.BAR)
    .query(
        Select.builder()
            .select(Order::getStatus)
            .select(Aggregate.sum(Order::getTotal).as("revenue"))
            .from(Order.class)
            .groupBy(Order::getStatus)
            .build()
    )
    .labelColumn("status")
    .dataset(ChartDataset.of("revenue")
        .label("Total Revenue")
        .backgroundColor("#4e79a7")
        .build())
    .build();

// 3. Assemble the dashboard
Dashboard dashboard = Dashboard.builder()
    .title("Sales Overview")
    .layout(DashboardLayout.GRID_2_COLS)
    .addWidget(revenueWidget)
    .build();

// 4. Render
String html = DashboardRenderer.builder()
    .dashboard(dashboard)
    .queryRunner(runner)
    .build()
    .renderHtml();
```

### Serving from Spring MVC

```java
@GetMapping(value = "/dashboard", produces = MediaType.TEXT_HTML_VALUE)
@ResponseBody
public String dashboard() {
    return DashboardRenderer.builder()
        .dashboard(myDashboard)
        .queryRunner(myRunner)
        .build()
        .renderHtml();
}
```

### Embedding in an existing page (Thymeleaf / JSP)

```java
// Returns only the <div> grid + <script> block — no <html>/<head> wrappers
String fragment = DashboardRenderer.builder()
    .dashboard(myDashboard)
    .queryRunner(myRunner)
    .build()
    .renderFragment();
```

### Custom Chart.js URL (offline / self-hosted)

By default, Chart.js is loaded from `https://cdn.jsdelivr.net/npm/chart.js`.
Override this for offline or self-hosted deployments:

```java
DashboardRenderer.builder()
    .dashboard(myDashboard)
    .queryRunner(myRunner)
    .chartJsUrl("/static/js/chart.umd.min.js")
    .build()
    .renderHtml();
```

### Multi-dataset charts

Call `.dataset(...)` multiple times on a widget to produce grouped / side-by-side series:

```java
DashboardWidget multiWidget = DashboardWidget.builder()
    .title("Revenue vs Order Count per Customer")
    .chartType(ChartType.BAR)
    .query(monthlyQuery)
    .labelColumn("customer_id")
    .dataset(ChartDataset.of("revenue")
        .label("Revenue").backgroundColor("#4e79a7").build())
    .dataset(ChartDataset.of("order_count")
        .label("Order Count").backgroundColor("#f28e2b").build())
    .build();
```

### Chart Options

Fine-tune each widget with `ChartOptions`:

```java
.chartOptions(ChartOptions.builder()
    .xAxisLabel("Month")
    .yAxisLabel("Revenue (USD)")
    .legendPosition(ChartOptions.LegendPosition.BOTTOM)
    .stacked(true)
    .build())
```

### Dashboard Layout Options

| Layout | Description |
|---|---|
| `SINGLE_COL` | One chart per row |
| `GRID_2_COLS` | Two charts per row *(default)* |
| `GRID_3_COLS` | Three charts per row |
| `MASONRY` | Auto-fit responsive grid |

---

## AI *(querier-ai)*

> **Requires** the `querier-ai` artifact. See [Installation](#installation).

Querier AI lets you translate **natural-language prompts** into validated Querier `Select` objects
or fully compiled `Dashboard` instances — no SQL writing required.

### Architecture: SQL-first, BYOK

The AI module is **provider-neutral** and works with any OpenAI-compatible endpoint
(OpenAI, Azure OpenAI, Mistral, Ollama, …). You supply your own API key via environment variables.

The pipeline for natural-language queries is:
1. Ask the AI to produce standard SQL (leverages the model's extensive SQL training)
2. Validate the SQL against your declared schema (security boundary — no hallucinated tables/columns)
3. Parse the SQL into a `Select` object (deterministic, no AI involved)

### Configuration

```java
AiClient client = AiClient.builder()
    .apiKey(System.getenv("QUERIER_AI_API_KEY"))
    .baseUrl(System.getenv("QUERIER_AI_BASE_URL"))   // e.g. https://api.openai.com/v1
    .model(System.getenv("QUERIER_AI_MODEL"))         // e.g. gpt-4o-mini
    .build();

QuerierAi ai = QuerierAi.builder()
    .client(client)
    .retryPolicy(RetryPolicy.ofMaxAttempts(2))        // auto-correct invalid SQL, optional
    .build();
```

The three environment variables used by default:

| Variable | Description |
|---|---|
| `QUERIER_AI_API_KEY` | API key for authentication |
| `QUERIER_AI_BASE_URL` | Base URL of the OpenAI-compatible endpoint |
| `QUERIER_AI_MODEL` | Model identifier (e.g. `gpt-4o`, `mistral-large-latest`) |

You can also load them automatically with `AiEndpoint.fromEnvironment()`.

### Defining the Schema Context

The AI only sees the tables and columns you explicitly expose. Use `QueryRequest.entities()` to
auto-inspect your annotated entity classes:

```java
QueryRequest request = QueryRequest.builder()
    .prompt("Show the top 10 customers by revenue in the last 30 days")
    .entities(Customer.class, Order.class)
    .build();
```

Or build an `AiSchemaContext` manually for full control.

### Natural-Language Queries

```java
QueryResult result = ai.query(request);

if (result.isValid()) {
    Select select = result.query().get();
    // use select with Spring JDBC, Hibernate, jOOQ, etc.
    Select.PositionalSqlAndParams sp = select.toPositionalSql();
    List<Map<String, Object>> rows = jdbcTemplate.queryForList(sp.sql(), sp.params().toArray());
} else {
    result.validationIssues().forEach(System.err::println);
}
```

If the prompt is ambiguous, `result.interpretationNote()` contains the AI's explanation.

### AI-Powered Dashboards *(requires querier-ui)*

```java
QueryRequest request = QueryRequest.builder()
    .prompt("Create a sales overview dashboard with revenue by status and top customers")
    .entities(Customer.class, Order.class)
    .build();

DashboardResult result = ai.dashboard(request);

if (result.isValid()) {
    Dashboard dashboard = result.dashboard().get();
    String html = DashboardRenderer.builder()
        .dashboard(dashboard)
        .queryRunner(myRunner)
        .build()
        .renderHtml();
}
```

### Semantic Annotations

Enrich your entity classes with the `@Semantic` annotation to help the AI map natural-language
terms to the correct columns:

```java
@Semantic(description = "A customer purchase", synonyms = {"sale", "transaction"})
@Table("orders")
public class Order {

    @Semantic(
        description = "Total monetary value of the order",
        synonyms = {"revenue", "sales value", "order value"}
    )
    public BigDecimal getTotal() { return total; }

    @Semantic(description = "Current state of the order", synonyms = {"state", "order state"})
    public OrderStatus getStatus() { return status; }
}
```

The annotation can be placed on the entity class, a getter method, or the backing field.

### Retry Policy

When the AI returns SQL that references unknown columns or tables, `querier-ai` can automatically
send a correction request:

```java
QuerierAi ai = QuerierAi.builder()
    .client(client)
    .retryPolicy(RetryPolicy.ofMaxAttempts(2)) // up to 2 correction attempts
    .build();
```

Use `RetryPolicy.disabled()` (the default) to turn off retries.

---

## API Reference

### Core Classes (`querier-core`)

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
- **`DbFunction`** - Type-safe SQL function call builder; supports vendor dialect factories and fully custom resolvers
- **`DbFunction.Arg`** - Sealed interface for function arguments: `Col` (column ref), `Val` (bound value), `Lit` (string literal), `Nested` (inner function), `RawSql` (verbatim SQL)
- **`DbFunction.Resolved`** - Record holding the rendered SQL fragment and its parameter map
- **`DbFunction.Resolver`** - Functional interface for custom SQL resolution logic

### Vendor Dialect Classes (`querier-core`)

- **`PgDialectFunctions`** - PostgreSQL-specific SQL functions (date, string, array, JSON, math, window)
- **`MySqlDialectFunctions`** - MySQL / MariaDB-specific SQL functions (date, string, JSON, math, conditional)
- **`MsSqlDialectFunctions`** - SQL Server / Azure SQL-specific SQL functions (date, string, JSON, math, conditional, window)
- **`OracleDialectFunctions`** - Oracle Database-specific SQL functions (date, string, JSON, math, conditional, window)

### Dashboard Classes (`querier-ui`)

- **`Dashboard`** - Top-level dashboard definition (title, layout, widgets)
- **`DashboardWidget`** - A single chart widget binding a `Select` query to a chart type and dataset(s)
- **`DashboardRenderer`** - Executes queries and renders the HTML output
- **`QueryRunner`** - Functional interface bridging Querier to your database execution layer
- **`ChartDataset`** - Maps a result-set column to Chart.js dataset properties (label, colors, fill)
- **`ChartData`** - Internal model representing the Chart.js `data` object (labels + datasets)
- **`ChartDataMapper`** - Transforms raw query rows into `ChartData`
- **`ChartOptions`** - Chart.js options (legend position, axis labels, stacked mode, responsive)
- **`ChartType`** - Enum of supported Chart.js chart types
- **`DashboardLayout`** - Enum for the dashboard grid layout

### AI Classes (`querier-ai`)

- **`QuerierAi`** - Primary entry point: `.query()` for natural-language → `Select`, `.dashboard()` for AI-generated dashboards
- **`AiClient`** - Provider-neutral interface to any OpenAI-compatible endpoint
- **`AiEndpoint`** - Immutable endpoint configuration (base URL, API key, model, timeouts, network proxy)
- **`AiCapabilities`** - Declares which features the configured endpoint/model supports
- **`QueryRequest`** - Input object containing the natural-language prompt, schema context, and request options
- **`QueryResult`** - Result of a `.query()` call — holds the compiled `Select` or validation issues
- **`DashboardResult`** - Result of a `.dashboard()` call — holds the compiled `Dashboard` or validation issues
- **`AiSchemaContext`** - The allowed schema boundary sent to the AI (tables + columns)
- **`AiSchemaInspector`** - Reflects on entity classes to produce `AiSchemaContext` automatically
- **`RetryPolicy`** - Configures automatic SQL correction retry attempts
- **`@Semantic`** - Annotation to enrich entity classes/fields with business-domain descriptions and synonyms for improved AI accuracy

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

---

## Building from Source

```bash
# Clone the repository
git clone https://github.com/AytmaTech/querier.git
cd querier

# Build all modules
mvn clean install

# Build only core
mvn clean install -pl querier-core

# Build only the AI module
mvn clean install -pl querier-ai

# Run tests
mvn test
```

---

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or suggestions, please open an issue on GitHub.