/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional informatiON regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, VersiON 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed ON an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.operators;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.MemoryLimits.assertMaxBytesAllocated;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.projection.LimitDistinctProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.TableInfo;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;

public class LogicalPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;
    private TableStats tableStats;

    @Before
    public void prepare() throws IOException {
        tableStats = new TableStats();
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .setTableStats(tableStats)
            .addView(new RelationName("doc", "v2"), "SELECT a, x FROM doc.t1")
            .addView(new RelationName("doc", "v3"), "SELECT a, x FROM doc.t1")
            .build();
    }

    private LogicalPlan plan(String statement) {
        return assertMaxBytesAllocated(ByteSizeUnit.MB.toBytes(28), () -> sqlExecutor.logicalPlan(statement));
    }

    @Test
    public void test_collect_derives_estimated_size_per_row_from_stats_and_types() {
        // no stats -> size derived FROM fixed with type
        LogicalPlan plan = plan("SELECT x FROM t1");
        assertThat(plan.estimatedRowSize()).isEqualTo((long) DataTypes.INTEGER.fixedSize());

        TableInfo t1 = sqlExecutor.resolveTableInfo("t1");
        ColumnStats<Integer> columnStats = new ColumnStats<>(
            0.0, 50L, 2, DataTypes.INTEGER, MostCommonValues.EMPTY, List.of());
        tableStats.updateTableStats(Map.of(t1.ident(), new Stats(2L, 100L, Map.of(new ColumnIdent("x"), columnStats))));

        // stats present -> size derived FROM them (although bogus fake stats in this case)
        plan = plan("SELECT x FROM t1");
        assertThat(plan.estimatedRowSize()).isEqualTo(50L);
    }

    @Test
    public void testAvgWindowFunction() {
        LogicalPlan plan = plan("SELECT avg(x) OVER() FROM t1");
        assertThat(plan).isEqualTo(
            """
            Eval[avg(x) OVER ()]
              └ WindowAgg[x, avg(x) OVER ()]
                └ Collect[doc.t1 | [x] | true]
            """
        );
    }

    @Test
    public void testAggregationOnTableFunction() throws Exception {
        LogicalPlan plan = plan("SELECT max(unnest) FROM unnest([1, 2, 3])");
        assertThat(plan).isEqualTo(
            """
            HashAggregate[max(unnest)]
              └ TableFunction[unnest | [unnest] | true]
            """
        );
    }

    @Test
    public void testQTFWithOrderBy() throws Exception {
        LogicalPlan plan = plan("SELECT a, x FROM t1 ORDER BY a");
        assertThat(plan).isEqualTo(
            """
            Fetch[a, x]
              └ OrderBy[a ASC]
                └ Collect[doc.t1 | [_fetchid, a] | true]
            """
        );
    }

    @Test
    public void testQTFWithOrderByAndAlias() throws Exception {
        LogicalPlan plan = plan("SELECT a, x FROM t1 as t ORDER BY a");
        assertThat(plan).isEqualTo(
            """
            Fetch[a, x]
              └ Rename[t._fetchid, a] AS t
                └ OrderBy[a ASC]
                  └ Collect[doc.t1 | [_fetchid, a] | true]
            """
        );
    }

    @Test
    public void testQTFWithoutOrderBy() throws Exception {
        LogicalPlan plan = plan("SELECT a, x FROM t1");
        assertThat(plan).isEqualTo("Collect[doc.t1 | [a, x] | true]");
    }

    @Test
    public void testSimpleSelectQAFAndLimit() throws Exception {
        LogicalPlan plan = plan("SELECT a FROM t1 ORDER BY a LIMIT 10 offset 5");
        assertThat(plan).isEqualTo(
            """
            Limit[10::bigint;5::bigint]
              └ OrderBy[a ASC]
                └ Collect[doc.t1 | [a] | true]
            """
        );
    }

    @Test
    public void testSelectOnVirtualTableWithOrderBy() throws Exception {
        LogicalPlan plan = plan(
            """
            SELECT a, x FROM (
              SELECT a, x FROM t1 ORDER BY a LIMIT 3) tt
            ORDER BY x DESC LIMIT 1
            """
        );
        assertThat(plan).isEqualTo(
            """
            Rename[a, x] AS tt
              └ Limit[1::bigint;0]
                └ OrderBy[x DESC]
                  └ Fetch[a, x]
                    └ Limit[3::bigint;0]
                      └ OrderBy[a ASC]
                        └ Collect[doc.t1 | [_fetchid, a] | true]
            """);
    }

    @Test
    public void testIntermediateFetch() throws Exception {
        LogicalPlan plan = plan("SELECT sum(x) FROM (SELECT x FROM t1 LIMIT 10) tt");
        assertThat(plan).isEqualTo(
            """
            HashAggregate[sum(x)]
              └ Rename[x] AS tt
                └ Fetch[x]
                  └ Limit[10::bigint;0]
                    └ Collect[doc.t1 | [_fetchid] | true]
            """);
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        LogicalPlan plan = plan("SELECT min(a), min(x) FROM t1 HAVING min(x) < 33 and max(x) > 100");
        assertThat(plan).isEqualTo(
            """
            Eval[min(a), min(x)]
              └ Filter[((min(x) < 33) AND (max(x) > 100))]
                └ HashAggregate[min(a), min(x), max(x)]
                  └ Collect[doc.t1 | [a, x] | true]
            """
        );
    }

    @Test
    public void testHavingGlobalAggregationAndRelationAlias() throws Exception {
        LogicalPlan plan = plan("SELECT min(a), min(x) FROM t1 as tt HAVING min(tt.x) < 33 and max(tt.x) > 100");
        assertThat(plan).isEqualTo(
            """
            Eval[min(a), min(x)]
              └ Filter[((min(x) < 33) AND (max(x) > 100))]
                └ HashAggregate[min(a), min(x), max(x)]
                  └ Rename[a, x] AS tt
                    └ Collect[doc.t1 | [a, x] | true]
            """);
    }

    @Test
    public void testSelectCountStarIsOptimized() throws Exception {
        LogicalPlan plan = plan("SELECT count(*) FROM t1 WHERE x > 10");
        assertThat(plan).isEqualTo("Count[doc.t1 | (x > 10)]");
    }

    @Test
    public void test_select_count_star_on_aliased_table_is_optimized() throws Exception {
        LogicalPlan plan = plan("SELECT count(*) FROM t1 as t");
        assertThat(plan).isEqualTo("Count[doc.t1 | true]");
    }

    @Test
    public void test_select_count_star_is_optimized_if_there_is_a_single_agg_in_select_list() {
        LogicalPlan plan = plan("SELECT COUNT(*), COUNT(x) FROM t1 WHERE x > 10");
        assertThat(plan).isEqualTo(
            """
            HashAggregate[count(*), count(x)]
              └ Collect[doc.t1 | [x] | (x > 10)]
            """
        );
    }

    @Test
    public void testSelectCountStarIsOptimizedOnNestedSubqueries() throws Exception {
        LogicalPlan plan = plan("SELECT * FROM t1 WHERE x > (SELECT 1 FROM t1 WHERE x > (SELECT count(*) FROM t2 LIMIT 1)::integer)");
        // instead of a Collect plan, this must result in a CountPlan through optimization
        assertThat(plan).hasOperators(
            "MultiPhase",
            "  └ Collect[doc.t1 | [a, x, i] | (x > (SELECT 1 FROM (doc.t1)))]",
            "  └ Limit[2::bigint;0::bigint]",
            "    └ MultiPhase",
            "      └ Eval[1]",
            "        └ Collect[doc.t1 | [1, x] | (x > cast((SELECT count(*) FROM (doc.t2)) AS integer))]",
            "      └ Limit[2::bigint;0::bigint]",
            "        └ Limit[1::bigint;0]",
            "          └ Count[doc.t2 | true]"
        );
    }

    @Test
    public void testSelectCountStarIsOptimizedInsideRelations() {
        LogicalPlan plan = plan(
            """
            SELECT t2.i, cnt FROM
              (SELECT count(*) as cnt FROM t1) t1
            JOIN
              (SELECT i FROM t2 LIMIT 1) t2
            ON t1.cnt = t2.i::long
            """
        );
        assertThat(plan).isEqualTo(
            """
            Eval[i, cnt]
              └ HashJoin[(cnt = cast(i AS bigint))]
                ├ Rename[cnt] AS t1
                │  └ Eval[count(*) AS cnt]
                │    └ Count[doc.t1 | true]
                └ Rename[i] AS t2
                  └ Fetch[i]
                    └ Limit[1::bigint;0]
                      └ Collect[doc.t2 | [_fetchid] | true]
            """);
    }

    @Test
    public void testJoinTwoTables() {
        LogicalPlan plan = plan(
            """
            SELECT t1.x, t1.a, t2.y
            FROM t1
            INNER JOIN t2 ON t1.x = t2.y
            ORDER BY t1.x
            LIMIT 10
             """);
        assertThat(plan).isEqualTo(
            """
            Fetch[x, a, y]
              └ Limit[10::bigint;0]
                └ OrderBy[x ASC]
                  └ HashJoin[(x = y)]
                    ├ Collect[doc.t1 | [_fetchid, x] | true]
                    └ Collect[doc.t2 | [y] | true]
            """);
    }

    @Test
    public void testScoreColumnIsCollectedNotFetched() throws Exception {
        LogicalPlan plan = plan("SELECT x, _score FROM t1");
        assertThat(plan).isEqualTo("Collect[doc.t1 | [x, _score] | true]");
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyApplied() {
        LogicalPlan plan = plan("SELECT x FROM t1 WHERE x in (SELECT x FROM t1)");
        assertThat(plan.dependencies().entrySet()).hasSize(1);
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan).isEqualTo(
            """
            OrderBy[x ASC]
              └ Collect[doc.t1 | [x] | true]
            """);
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderBy() {
        LogicalPlan plan = plan("SELECT x FROM t1 WHERE x in (SELECT x FROM t1 ORDER BY 1 DESC LIMIT 10)");
        assertThat(plan.dependencies().entrySet()).hasSize(1);
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan).isEqualTo(
            """
            Limit[10::bigint;0]
              └ OrderBy[x DESC]
                └ Collect[doc.t1 | [x] | true]
            """);
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderByOnDifferentField() {
        LogicalPlan plan = plan("SELECT x FROM t1 WHERE x in (SELECT x FROM t1 ORDER BY a DESC LIMIT 10)");
        assertThat(plan.dependencies().entrySet()).hasSize(1);
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan).isEqualTo(
            """
            Eval[x]
              └ OrderBy[x ASC]
                └ Limit[10::bigint;0]
                  └ OrderBy[a DESC]
                    └ Collect[doc.t1 | [x, a] | true]
            """);
    }

    @Test
    public void test_optimize_for_in_subquery_only_operates_on_primitive_types() {
        LogicalPlan plan = plan("SELECT array(SELECT {a = x} FROM t1)");
        assertThat(plan.dependencies().entrySet()).hasSize(1);
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan).isEqualTo("Collect[doc.t1 | [_map('a', x)] | true]");
    }

    @Test
    public void testParentQueryIsPushedDownAndMergedIntoSubRelationWhereClause() {
        LogicalPlan plan = plan("SELECT * FROM " +
                                " (SELECT a, i FROM t1 ORDER BY a LIMIT 5) t1 " +
                                "INNER JOIN" +
                                " (SELECT b, i FROM t2 WHERE b > '10') t2 " +
                                "ON t1.i = t2.i WHERE t1.a > '50' and t2.b > '100' " +
                                "LIMIT 10");
        assertThat(plan).isEqualTo(
            """
            Fetch[a, i, b, i]
              └ Limit[10::bigint;0]
                └ HashJoin[(i = i)]
                  ├ Rename[a, i] AS t1
                  │  └ Filter[(a > '50')]
                  │    └ Fetch[a, i]
                  │      └ Limit[5::bigint;0]
                  │        └ OrderBy[a ASC]
                  │          └ Collect[doc.t1 | [_fetchid, a] | true]
                  └ Rename[t2._fetchid, i] AS t2
                    └ Collect[doc.t2 | [_fetchid, i] | ((b > '100') AND (b > '10'))]
            """);
    }

    @Test
    public void testPlanOfJoinedViewsHasBoundaryWithViewOutputs() {
        LogicalPlan plan = plan(
            """
            SELECT v2.x, v2.a, v3.x, v3.a
            FROM v2
            INNER JOIN v3
            ON v2.x= v3.x
            """);
        assertThat(plan).isEqualTo(
            """
            Eval[x, a, x, a]
              └ HashJoin[(x = x)]
                ├ Rename[a, x] AS doc.v2
                │  └ Collect[doc.t1 | [a, x] | true]
                └ Rename[a, x] AS doc.v3
                  └ Collect[doc.t1 | [a, x] | true]
            """);
    }

    @Test
    public void testAliasedPrimaryKeyLookupHasGetPlan() {
        LogicalPlan plan = plan("SELECT name FROM users u WHERE id = 1");
        assertThat(plan).isEqualTo(
            "Rename[name] AS u\n" +
            "  └ Get[doc.users | name | DocKeys{1::bigint} | (id = 1::bigint)]");
    }

    @Test
    public void test_limit_distinct_limits_outputs_to_the_group_keys_if_source_has_more_outputs() {
        String statement = """
                            SELECT name, other_id
                            FROM (SELECT name, awesome, other_id FROM users) u
                            GROUP BY name, other_id LIMIT 20
                           """;
        LogicalPlan plan = plan(statement);
        assertThat(plan).isEqualTo(
            """
            LimitDistinct[20::bigint;0 | [name, other_id]]
              └ Rename[name, other_id] AS u
                └ Collect[doc.users | [name, other_id] | true]
            """
        );
        io.crate.planner.node.dql.Collect collect = sqlExecutor.plan(statement);
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections).satisfiesExactly(
            p -> assertThat(p).isExactlyInstanceOf(LimitDistinctProjection.class),
            p -> assertThat(p).isExactlyInstanceOf(LimitDistinctProjection.class)
        );
        assertThat(projections.get(0).requiredGranularity()).isEqualTo(RowGranularity.SHARD);
        assertThat(projections.get(1).requiredGranularity()).isEqualTo(RowGranularity.CLUSTER);
    }

    @Test
    public void test_limit_on_join_is_rewritten_to_query_then_fetch() {
        LogicalPlan plan = plan("SELECT * FROM t1, t2 LIMIT 3");
        assertThat(plan).isEqualTo(
            """
            Fetch[a, x, i, b, y, i]
              └ Limit[3::bigint;0]
                └ NestedLoopJoin[CROSS]
                  ├ Collect[doc.t1 | [_fetchid] | true]
                  └ Collect[doc.t2 | [_fetchid] | true]
            """
        );
    }

    @Test
    public void test_limit_on_hash_join_is_rewritten_to_query_then_fetch() {
        LogicalPlan plan = plan("SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b LIMIT 3");
        assertThat(plan).isEqualTo(
            """
            Fetch[a, x, i, b, y, i]
              └ Limit[3::bigint;0]
                └ HashJoin[(a = b)]
                  ├ Collect[doc.t1 | [_fetchid, a] | true]
                  └ Collect[doc.t2 | [_fetchid, b] | true]
            """
        );
    }

    @Test
    public void test_unused_table_function_in_subquery_is_not_pruned() {
        LogicalPlan plan = plan("SELECT name FROM (SELECT name, unnest(counters), text FROM users) u");
        assertThat(plan).isEqualTo(
            """
            Rename[name] AS u
              └ Eval[name]
                └ ProjectSet[unnest(counters), name]
                  └ Collect[doc.users | [counters, name] | true]
            """
        );
    }

    @Test
    public void test_group_by_with_alias_and_limit_distinct_rewrite_creates_valid_plan() {
        TableInfo t1 = sqlExecutor.resolveTableInfo("t1");
        tableStats.updateTableStats(Map.of(t1.ident(), new Stats(100L, 100L, Map.of())));
        LogicalPlan plan = plan("SELECT a as b FROM doc.t1 GROUP BY a LIMIT 10");
        assertThat(plan).isEqualTo(
            """
            Eval[a AS b]
              └ LimitDistinct[10::bigint;0 | [a]]
                └ Collect[doc.t1 | [a] | true]
            """
        );
    }

    @Test
    public void test_query_uses_fetch_if_there_is_a_nested_loop_join_where_only_one_side_can_utilize_fetch() throws Exception {
        // (uses like to force NL instead of hashjoin)
        LogicalPlan plan = plan("""
            SELECT * FROM (SELECT distinct name FROM users) u
            INNER JOIN t1 ON t1.a like u.name
            LIMIT 10
            """);
        assertThat(plan).isEqualTo(
            """
            Fetch[name, a, x, i]
              └ Limit[10::bigint;0]
                └ NestedLoopJoin[INNER | (a LIKE name)]
                  ├ Rename[name] AS u
                  │  └ GroupHashAggregate[name]
                  │    └ Collect[doc.users | [name] | true]
                  └ Collect[doc.t1 | [_fetchid, a] | true]
            """
        );
    }

    @Test
    public void test_query_uses_fetch_if_there_is_a_hash_join_where_only_one_side_can_utilize_fetch() throws Exception {
        LogicalPlan plan = plan("""
            SELECT * FROM (SELECT distinct name FROM users) u
            INNER JOIN t1 ON t1.a = u.name
            LIMIT 10
            """);
        assertThat(plan).isEqualTo(
            """
            Fetch[name, a, x, i]
              └ Limit[10::bigint;0]
                └ HashJoin[(a = name)]
                  ├ Rename[name] AS u
                  │  └ GroupHashAggregate[name]
                  │    └ Collect[doc.users | [name] | true]
                  └ Collect[doc.t1 | [_fetchid, a] | true]
            """
        );
    }

    @Test
    public void test_orderBy_not_optimized_for_array_subquery_expression() {
        LogicalPlan plan = plan("SELECT array(SELECT x FROM t1 ORDER BY a DESC LIMIT 10)");
        assertThat(plan.dependencies().entrySet()).hasSize(1);
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan).isEqualTo(
            """
            Eval[x]
              └ Limit[10::bigint;0]
                └ OrderBy[a DESC]
                  └ Collect[doc.t1 | [x, a] | true]
            """);
    }

    @Test
    public void test_eval_qtf_doesnt_unwrap_non_fetchable_aliases() {
        // To reproduce https://github.com/crate/crate/issues/13414  we need:
        // 1. SELECT at least one fetchable column (t1.i) to really kick in Query-Then-Fetch execution, just LIMIT is not enough.
        // 2. SELECT used in join aliased column
        // 3. use virtual table (view or subselect)
        // 4. use LIMIT ON the whole query
        LogicalPlan plan = plan(
            """
            SELECT * FROM generate_series(1, 2)
            CROSS JOIN
            (SELECT t1.i, t2.y AS aliased FROM t1 INNER JOIN t2 ON t1.x = t2.y) v
            LIMIT 10
            """
        );
        assertThat(plan).isEqualTo(
            """
            Fetch[generate_series, i, aliased]
              └ Limit[10::bigint;0]
                └ NestedLoopJoin[CROSS]
                  ├ TableFunction[generate_series | [generate_series] | true]
                  └ Rename[v._fetchid, aliased] AS v
                    └ Eval[_fetchid, y AS aliased]
                      └ HashJoin[(x = y)]
                        ├ Collect[doc.t1 | [_fetchid, x] | true]
                        └ Collect[doc.t2 | [y] | true]
            """
        );
    }
}
