package com.aytmatech.querier.ai.analytics;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import com.aytmatech.querier.annotation.Table;
import java.util.List;
import org.junit.jupiter.api.Test;

class DashboardPlanValidatorTest {

  @Table("customers")
  private static class Customer {
    public String getName() {
      return null;
    }

    public String getEmail() {
      return null;
    }
  }

  private static final AiSchemaContext CTX = AiSchemaInspector.inspect(Customer.class);

  private static WidgetPlan validWidget() {
    return WidgetPlan.builder()
        .title("Customers")
        .chartType("BAR")
        .labelColumn("name")
        .rawSql("SELECT c.name, COUNT(*) AS cnt FROM customers c GROUP BY c.name")
        .dataset(new WidgetPlan.DatasetSpec("cnt", "Count"))
        .build();
  }

  private static DashboardPlan validPlan() {
    return DashboardPlan.builder()
        .title("Test")
        .layout("GRID_2_COLS")
        .widget(validWidget())
        .build();
  }

  @Test
  void validate_validPlan_returnsNoIssues() {
    List<String> issues = DashboardPlanValidator.validate(validPlan(), CTX);
    assertTrue(issues.isEmpty(), "Expected no issues but got: " + issues);
  }

  @Test
  void validate_blankTitle_returnsIssue() {
    DashboardPlan plan =
        DashboardPlan.builder().title(" ").layout("GRID_2_COLS").widget(validWidget()).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("title")));
  }

  @Test
  void validate_unknownLayout_returnsIssue() {
    DashboardPlan plan =
        DashboardPlan.builder().title("T").layout("INVALID_LAYOUT").widget(validWidget()).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("layout")));
  }

  @Test
  void validate_noWidgets_builderThrowsIllegalState() {
    assertThrows(IllegalStateException.class, () -> DashboardPlan.builder().title("T").build());
  }

  @Test
  void validate_widget_blankLabelColumn_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("")
            .rawSql("SELECT c.name FROM customers c")
            .dataset(new WidgetPlan.DatasetSpec("name", "Name"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("labelColumn")));
  }

  @Test
  void validate_widget_noDatasets_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("name")
            .rawSql("SELECT c.name FROM customers c")
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("dataset")));
  }

  @Test
  void validate_widget_blankDatasetColumn_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("name")
            .rawSql("SELECT c.name FROM customers c")
            .dataset(new WidgetPlan.DatasetSpec("", "Name"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("column")));
  }

  @Test
  void validate_widget_blankDatasetLabel_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("name")
            .rawSql("SELECT c.name FROM customers c")
            .dataset(new WidgetPlan.DatasetSpec("name", ""))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("label")));
  }

  @Test
  void validate_widget_unknownChartType_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("UNKNOWN_TYPE")
            .labelColumn("name")
            .rawSql("SELECT c.name FROM customers c")
            .dataset(new WidgetPlan.DatasetSpec("name", "Name"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("UNKNOWN_TYPE")));
  }

  @Test
  void validate_widget_pieChart_moreThanOneDataset_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("PIE")
            .labelColumn("name")
            .rawSql("SELECT c.name, COUNT(*) AS cnt, SUM(1) AS s FROM customers c GROUP BY c.name")
            .dataset(new WidgetPlan.DatasetSpec("cnt", "Count"))
            .dataset(new WidgetPlan.DatasetSpec("s", "Sum"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("PIE")));
  }

  @Test
  void validate_widget_barChart_multipleDatasets_ok() {
    WidgetPlan widget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("name")
            .rawSql("SELECT c.name, COUNT(*) AS cnt, MAX(1) AS mx FROM customers c GROUP BY c.name")
            .dataset(new WidgetPlan.DatasetSpec("cnt", "Count"))
            .dataset(new WidgetPlan.DatasetSpec("mx", "Max"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(widget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    // BAR chart with multiple datasets is allowed — no dataset-count issue expected
    assertTrue(issues.stream().noneMatch(i -> i.contains("dataset") && i.contains("one")));
  }

  @Test
  void validate_widget_rawSql_invalidSql_returnsIssue() {
    WidgetPlan badWidget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("col")
            .rawSql("SELECT o.id FROM unknown_table o")
            .dataset(new WidgetPlan.DatasetSpec("col", "Col"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(badWidget).build();

    List<String> issues = DashboardPlanValidator.validate(plan, CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("unknown_table")));
  }
}
