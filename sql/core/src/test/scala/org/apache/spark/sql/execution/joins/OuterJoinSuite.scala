/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

class OuterJoinSuite extends SparkPlanTest with SharedSQLContext {

  private lazy val leftLong = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0),
      Row(2, 100.0),
      Row(2, 1.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row(2, 1.0),
      Row(3, 3.0),
      Row(5, 1.0),
      Row(6, 6.0),
      Row(null, null)
    )), new StructType().add("a", IntegerType).add("b", DoubleType))

  private lazy val rightLong = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(0, 0.0),
      Row(2, 3.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row(2, -1.0),
      Row(2, -1.0),
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(5, 3.0),
      Row(7, 7.0),
      Row(null, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val conditionLong = {
    And((leftLong.col("a") === rightLong.col("c")).expr,
      LessThan(leftLong.col("b").expr, rightLong.col("d").expr))
  }

  private lazy val leftBytes = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row("1", 2.0),
      Row("2", 100.0),
      Row("2", 1.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row("2", 1.0),
      Row("3", 3.0),
      Row("5", 1.0),
      Row("6", 6.0),
      Row(null, null)
    )), new StructType().add("a", StringType).add("b", DoubleType))

  private lazy val rightBytes = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row("0", 0.0),
      Row("2", 3.0), // This row is duplicated to ensure that we will have multiple buffered matches
      Row("2", -1.0),
      Row("2", -1.0),
      Row("2", 3.0),
      Row("3", 2.0),
      Row("4", 1.0),
      Row("5", 3.0),
      Row("7", 7.0),
      Row(null, null)
    )), new StructType().add("c", StringType).add("d", DoubleType))

  private lazy val conditionBytes = {
    And((leftBytes.col("a") === rightBytes.col("c")).expr,
      LessThan(leftBytes.col("b").expr, rightBytes.col("d").expr))
  }

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testOuterJoin(
      testName: String,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      joinType: JoinType,
      outerJoinHashSameSide: Boolean,
      conditionLong: => Expression,
      expectedAnswer: Seq[Product]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(conditionLong))
      ExtractEquiJoinKeys.unapply(join)
    }

    if (joinType != FullOuter) {
      test(s"$testName using ShuffledHashJoin") {
        extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            val buildSide = {
              if (joinType == LeftOuter)
                if (outerJoinHashSameSide) BuildLeft else BuildRight
              else
                if (outerJoinHashSameSide) BuildRight else BuildLeft
            }
            checkAnswer2(leftRows, rightRows, (leftLong: SparkPlan, rightLong: SparkPlan) =>
              EnsureRequirements(spark.sessionState.conf).apply(
                ShuffledHashJoinExec(
                  leftKeys, rightKeys, joinType, buildSide, boundCondition, leftLong, rightLong)),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
      }
    }
    if (outerJoinHashSameSide) return

    if (joinType != FullOuter) {
      test(s"$testName using BroadcastHashJoin") {
        val buildSide = joinType match {
          case LeftOuter => BuildRight
          case RightOuter => BuildLeft
          case _ => fail(s"Unsupported join type $joinType")
        }
        extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            checkAnswer2(leftRows, rightRows, (leftLong: SparkPlan, rightLong: SparkPlan) =>
              BroadcastHashJoinExec(
                leftKeys, rightKeys, joinType, buildSide, boundCondition, leftLong, rightLong),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
      }
    }

    test(s"$testName using SortMergeJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftLong: SparkPlan, rightLong: SparkPlan) =>
            EnsureRequirements(spark.sessionState.conf).apply(
              SortMergeJoinExec(leftKeys, rightKeys, joinType, boundCondition,
                leftLong, rightLong)),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build leftLong") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (leftLong: SparkPlan, rightLong: SparkPlan) =>
          BroadcastNestedLoopJoinExec(leftLong, rightLong, BuildLeft, joinType,
            Some(conditionLong)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build rightLong") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (leftLong: SparkPlan, rightLong: SparkPlan) =>
          BroadcastNestedLoopJoinExec(leftLong, rightLong, BuildRight, joinType,
            Some(conditionLong)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }
  }

  // --- Basic outer joins ------------------------------------------------------------------------
  val outerJoinHashSameSideSeq = Array(false, true)
  for (outerJoinHashSameSide <- outerJoinHashSameSideSeq) {
    testOuterJoin(
      "basic left outer join with long hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftLong,
      rightLong,
      LeftOuter,
      outerJoinHashSameSide,
      conditionLong,
      Seq(
        (null, null, null, null),
        (1, 2.0, null, null),
        (2, 100.0, null, null),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (3, 3.0, null, null),
        (5, 1.0, 5, 3.0),
        (6, 6.0, null, null)
      )
    )

    testOuterJoin(
      "basic right outer join with long hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftLong,
      rightLong,
      RightOuter,
      outerJoinHashSameSide,
      conditionLong,
      Seq(
        (null, null, null, null),
        (null, null, 0, 0.0),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (null, null, 2, -1.0),
        (null, null, 2, -1.0),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (null, null, 3, 2.0),
        (null, null, 4, 1.0),
        (5, 1.0, 5, 3.0),
        (null, null, 7, 7.0)
      )
    )

    testOuterJoin(
      "basic full outer join with long hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftLong,
      rightLong,
      FullOuter,
      outerJoinHashSameSide,
      conditionLong,
      Seq(
        (1, 2.0, null, null),
        (null, null, 2, -1.0),
        (null, null, 2, -1.0),
        (2, 100.0, null, null),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (2, 1.0, 2, 3.0),
        (3, 3.0, null, null),
        (5, 1.0, 5, 3.0),
        (6, 6.0, null, null),
        (null, null, 0, 0.0),
        (null, null, 3, 2.0),
        (null, null, 4, 1.0),
        (null, null, 7, 7.0),
        (null, null, null, null),
        (null, null, null, null)
      )
    )

    testOuterJoin(
      "basic lef outer join with bytes hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftBytes,
      rightBytes,
      LeftOuter,
      outerJoinHashSameSide,
      conditionBytes,
      Seq(
        (null, null, null, null),
        ("1", 2.0, null, null),
        ("2", 100.0, null, null),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        ("3", 3.0, null, null),
        ("5", 1.0, "5", 3.0),
        ("6", 6.0, null, null)
      )
    )

    testOuterJoin(
      "basic right outer join with bytes hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftBytes,
      rightBytes,
      RightOuter,
      outerJoinHashSameSide,
      conditionBytes,
      Seq(
        (null, null, null, null),
        (null, null, "0", 0.0),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        (null, null, "2", -1.0),
        (null, null, "2", -1.0),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        (null, null, "3", 2.0),
        (null, null, "4", 1.0),
        ("5", 1.0, "5", 3.0),
        (null, null, "7", 7.0)
      )
    )

    testOuterJoin(
      "basic full outer join with bytes hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftBytes,
      rightBytes,
      FullOuter,
      outerJoinHashSameSide,
      conditionBytes,
      Seq(
        ("1", 2.0, null, null),
        (null, null, "2", -1.0),
        (null, null, "2", -1.0),
        ("2", 100.0, null, null),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        ("2", 1.0, "2", 3.0),
        ("3", 3.0, null, null),
        ("5", 1.0, "5", 3.0),
        ("6", 6.0, null, null),
        (null, null, "0", 0.0),
        (null, null, "3", 2.0),
        (null, null, "4", 1.0),
        (null, null, "7", 7.0),
        (null, null, null, null),
        (null, null, null, null)
      )
    )

    // --- Both inputs empty ---------------------------------------------------------------------

    testOuterJoin(
      "left outer join with both inputs empty and long hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftLong.filter("false"),
      rightLong.filter("false"),
      LeftOuter,
      outerJoinHashSameSide,
      conditionLong,
      Seq.empty
    )

    testOuterJoin(
      "right outer join with both inputs empty and long hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftLong.filter("false"),
      rightLong.filter("false"),
      RightOuter,
      outerJoinHashSameSide,
      conditionLong,
      Seq.empty
    )

    testOuterJoin(
      "full outer join with both inputs empty and long hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftLong.filter("false"),
      rightLong.filter("false"),
      FullOuter,
      outerJoinHashSameSide,
      conditionLong,
      Seq.empty
    )

    testOuterJoin(
      "leftLong outer join with both inputs empty and bytes hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftBytes.filter("false"),
      rightBytes.filter("false"),
      LeftOuter,
      outerJoinHashSameSide,
      conditionBytes,
      Seq.empty
    )

    testOuterJoin(
      "rightLong outer join with both inputs empty and bytes hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftBytes.filter("false"),
      rightBytes.filter("false"),
      RightOuter,
      outerJoinHashSameSide,
      conditionBytes,
      Seq.empty
    )

    testOuterJoin(
      "full outer join with both inputs empty and bytes hash key " +
        s"(outerJoinHashSameSide = $outerJoinHashSameSide)",
      leftBytes.filter("false"),
      rightBytes.filter("false"),
      FullOuter,
      outerJoinHashSameSide,
      conditionBytes,
      Seq.empty
    )
  }
}
