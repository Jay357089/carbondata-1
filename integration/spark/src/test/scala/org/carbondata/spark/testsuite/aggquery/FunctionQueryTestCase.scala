/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.spark.testsuite.aggquery

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

import org.scalatest.BeforeAndAfterAll

/**
 * test cases for function query
 */
class FunctionQueryTestCase extends QueryTest with BeforeAndAfterAll {
  // set test data path
  val pwd =new File(this.getClass.getResource("/").getPath + "/../../../")
    .getCanonicalPath
  val testData = pwd + "/spark/src/test/resources/functionData.csv"

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbonTable")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    sql("""
       CREATE TABLE carbonTable(ID int, date timeStamp, country string, name string,
         phonetype string, serialname string, salary double)
       STORED BY 'org.apache.carbondata.format'
        """)
    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE carbonTable")
  }

  test("group by with subString(TimeStamp)") {
    checkAnswer(
      sql("""SELECT substring(date,1,7), sum(ID), sum(salary) FROM carbonTable
         GROUP BY substring(date,1,7) ORDER BY substring(date,1,7)"""),
      Seq(Row("2015-07", 14.0, 60010.0),
        Row("2015-08",123.0, 45120.0),
        Row("2015-09", 120.0, 30118.0),
        Row("2015-10", 185.0, 30183.0))
    )
  }

  test("group by with concat(subString(TimeStamp),'string'))") {
    checkAnswer(
      sql("""SELECT concat(substring(date,1,7),'-21'), sum(ID), sum(salary) FROM carbonTable
         GROUP BY concat(substring(date,1,7),'-21') ORDER BY concat(substring(date,1,7),'-21')"""),
      Seq(Row("2015-07-21", 14.0, 60010.0),
        Row("2015-08-21",123.0, 45120.0),
        Row("2015-09-21", 120.0, 30118.0),
        Row("2015-10-21", 185.0, 30183.0))
    )
  }

  test("group by with subString(subString(TimeStamp))") {
    checkAnswer(
      sql("""SELECT substring(substring(date,1,7),1,4), sum(ID), sum(salary) FROM carbonTable
         GROUP BY substring(substring(date,1,7),1,4)
         ORDER BY substring(substring(date,1,7),1,4)"""),
      Seq(Row("2015", 442.0, 165431.0))
    )
  }

  test("group by with Cast(subString(TimeStamp)) as alias)") {
    checkAnswer(
      sql("""SELECT cast(substring(date,1,7) as String) as sub_date,
       sum(ID) as sum_id, sum(salary) as sum_salary FROM carbonTable
         GROUP BY cast(substring(date,1,7) as String)
         ORDER BY cast(substring(date,1,7) as String)"""),
      Seq(Row("2015-07", 14.0, 60010.0),
        Row("2015-08",123.0, 45120.0),
        Row("2015-09", 120.0, 30118.0),
        Row("2015-10", 185.0, 30183.0))
    )
  }

  test("group by with subString(String)") {
    checkAnswer(
      sql( """SELECT substring(phonetype,1,6), sum(ID), sum(salary) FROM carbonTable
         GROUP BY substring(phonetype,1,6) ORDER BY substring(phonetype,1,6)"""),
      Seq(Row("phone1", 1.0, 15000.0),
        Row("phone2", 103.0, 45100.0),
        Row("phone3", 42.0, 15041.0),
        Row("phone4", 58.0, 15057.0),
        Row("phone5", 62.0, 15061.0),
        Row("phone7", 138.0, 45135.0),
        Row("phone8", 38.0, 15037.0)
      )
    )
  }

  override def afterAll {
    sql("DROP TABLE carbonTable")
  }

}
