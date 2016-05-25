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
package org.carbondata.spark.util

import java.io.File

import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest

import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.spark.load.{CarbonLoadModel, CarbonLoaderUtil}

import org.scalatest.BeforeAndAfterAll

/**
 * test case for external column dictionary generation
 * also support complicated type
 */
class ExternalColumnDictionaryTestCase extends QueryTest with BeforeAndAfterAll {

  var extComplexRelation: CarbonRelation = _
  var filePath: String = _
  var pwd: String = _
  var complexfilePath: String = _
  var extColDictFilePath: String = _
  var header: String = _

  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    filePath = pwd + "/src/test/resources/sample.csv"
    complexfilePath = pwd + "/src/test/resources/complexdata2.csv"
    extColDictFilePath = "deviceInformationId:" + pwd +
      "/src/test/resources/deviceInformationId.csv," +
      "mobile:" + pwd + "/src/test/resources/mobile.csv," +
      "mac:" + pwd + "/src/test/resources/mac.csv," +
      "locationInfo:" + pwd + "/src/test/resources/locationInfo.csv"
    header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber"
  }

  def buildTable() = {
    try {
      sql("CREATE CUBE IF NOT EXISTS extComplextypes DIMENSIONS(deviceInformationId integer, channelsId string, ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, MAC array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) MEASURES(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [PARTITION_COUNT=1] )")
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog
    extComplexRelation = catalog.lookupRelation1(None, "extComplextypes", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
  }
  def buildCarbonLoadModel(relation: CarbonRelation,
                           filePath:String,
                           header: String,
                           extColFilePath: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    val table = relation.cubeMeta.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(",")
    carbonLoadModel.setComplexDelimiterLevel1("\\$")
    carbonLoadModel.setComplexDelimiterLevel2("\\:")
    carbonLoadModel.setColDictFilePath(extColFilePath)
    carbonLoadModel
  }

  override def beforeAll {
    buildTestData
    buildTable
    buildRelation
  }

  def checkDictionary(relation: CarbonRelation, columnName: String, value: String){
    val table = relation.cubeMeta.carbonTable
    val dimension = table.getDimensionByName(table.getFactTableName, columnName)
    val tableIdentifier = new CarbonTableIdentifier(table.getDatabaseName,table.getFactTableName)
    val columnIdentifier =  new DictionaryColumnUniqueIdentifier(tableIdentifier,
      dimension.getColumnId)
    val dict = CarbonLoaderUtil.getDictionary(columnIdentifier,
      CarbonHiveContext.hdfsCarbonBasePath)
    assert( dict.getSurrogateKey(value)!= CarbonCommonConstants.INVALID_SURROGATE_KEY)
  }

  test("[issue-126]Generate global dictionary from external column file") {
    // test external column file to generate global dict
    val carbonLoadModel = buildCarbonLoadModel(extComplexRelation, complexfilePath, header, extColDictFilePath)
    GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel, extComplexRelation.cubeMeta.dataPath)
    checkDictionary(extComplexRelation, "deviceInformationId", "10086")
  }
}
