package com.aytmatech.querier.dashboard;

import com.aytmatech.querier.Select;
import java.util.List;
import java.util.Map;

/** Functional interface for running a query and returning results as a list of maps. */
@FunctionalInterface
public interface QueryRunner {
  /**
   * Executes the given query and returns the results as a list of maps, where each map represents a
   * row in the result set with column names as keys and corresponding values.
   *
   * @param select the query to run, represented as a Querier Select object
   * @return a list of maps, where each map represents a row in the result set with column names as
   *     keys and corresponding values
   */
  List<Map<String, Object>> run(Select select);
}
