package com.aytmatech.querier.ai.analytics;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DashboardResult}.
 *
 * <p>The {@code dashboard} field requires a {@link com.aytmatech.querier.dashboard.Dashboard} from
 * the querier-ui module. Since querier-core does not depend on querier-ui, tests that require a
 * non-null {@code Dashboard} instance verify the builder contract through reflection by passing
 * {@code null} (the invalid path) and validate all other accessors independently.
 */
class DashboardResultTest {

  @Test
  void isValid_nullDashboard_returnsFalse() {
    DashboardResult result = DashboardResult.builder().prompt("test").build();
    assertFalse(result.isValid());
  }

  @Test
  void isValid_withValidationIssue_returnsFalse() {
    DashboardResult result =
        DashboardResult.builder().prompt("test").validationIssue("something wrong").build();
    assertFalse(result.isValid());
  }

  @Test
  void dashboard_whenNotSet_emptyOptional() {
    DashboardResult result = DashboardResult.builder().prompt("test").build();
    assertTrue(result.dashboard().isEmpty());
  }

  @Test
  void validationIssues_areImmutable() {
    DashboardResult result =
        DashboardResult.builder().prompt("test").validationIssue("issue").build();

    assertThrows(
        UnsupportedOperationException.class, () -> result.validationIssues().add("another"));
  }

  @Test
  void interpretationNote_whenSet_wrappedInOptional() {
    DashboardResult result =
        DashboardResult.builder().prompt("test").interpretationNote("interpreted as X").build();

    assertTrue(result.interpretationNote().isPresent());
    assertEquals("interpreted as X", result.interpretationNote().get());
  }

  @Test
  void interpretationNote_whenNotSet_emptyOptional() {
    DashboardResult result = DashboardResult.builder().prompt("test").build();
    assertTrue(result.interpretationNote().isEmpty());
  }

  @Test
  void prompt_isStored() {
    DashboardResult result = DashboardResult.builder().prompt("my prompt").build();
    assertEquals("my prompt", result.prompt());
  }

  @Test
  void validationIssues_multipleIssues_allStored() {
    DashboardResult result =
        DashboardResult.builder()
            .prompt("test")
            .validationIssue("issue 1")
            .validationIssue("issue 2")
            .build();

    assertEquals(2, result.validationIssues().size());
  }

  @Test
  void plan_whenSet_isStored() {
    WidgetPlan widget =
        WidgetPlan.builder()
            .title("W")
            .chartType("BAR")
            .labelColumn("col")
            .rawSql("SELECT 1")
            .dataset(new WidgetPlan.DatasetSpec("col", "L"))
            .build();
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(widget).build();

    DashboardResult result = DashboardResult.builder().prompt("test").plan(plan).build();

    assertSame(plan, result.plan());
  }
}
