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

package org.carbondata.spark.rdd

import java.io.InputStreamReader
import java.nio.charset.Charset
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.spark.load.{CarbonLoaderUtil, CarbonLoadModel}
import org.carbondata.spark.partition.reader.{CSVParser, CSVReader}
import org.carbondata.spark.util.GlobalDictionaryUtil
import org.carbondata.spark.util.GlobalDictionaryUtil._

/**
 * A partitioner partition by column.
 *
 * @constructor create a partitioner
 * @param numParts  the number of partitions
 */
class ColumnPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

trait GenericParser {
  val dimension: CarbonDimension
  def addChild(child: GenericParser): Unit
  def parseString(input: String): Unit
}

case class PrimitiveParser(dimension: CarbonDimension,
                           setOpt: Option[HashSet[String]]) extends GenericParser {
  val (hasDictEncoding, set: HashSet[String]) = setOpt match {
    case None => (false, new HashSet[String])
    case Some(x) => (true, x)
  }

  def addChild(child: GenericParser): Unit = {
  }

  def parseString(input: String): Unit = {
    if (hasDictEncoding) {
      set.add(input)
    }
  }
}

case class ArrayParser(val dimension: CarbonDimension,
                       val format: DataFormat) extends GenericParser {
  var children: GenericParser = _
  def addChild(child: GenericParser): Unit = {
    children = child
  }
  def parseString(input: String): Unit = {
    if (StringUtils.isNotEmpty(input)) {
      val splits = format.getSplits(input)
      if (ArrayUtils.isNotEmpty(splits)) {
        for (i <- 0 until splits.length) {
          children.parseString(splits(i))
        }
      }
    }
  }
}

case class StructParser(dimension: CarbonDimension,
                        format: DataFormat) extends GenericParser {
  val children = new ArrayBuffer[GenericParser]
  def addChild(child: GenericParser): Unit = {
    children += child
  }
  def parseString(input: String): Unit = {
    if (StringUtils.isNotEmpty(input)) {
      val splits = format.getSplits(input)
      val len = Math.min(children.length, splits.length)
      for (i <- 0 until len) {
        children(i).parseString(splits(i))
      }
    }
  }
}

case class DataFormat(delimiters: Array[String],
                      var delimiterIndex: Int,
                      patterns: Array[Pattern]) extends Serializable {
  self =>
  def getSplits(input: String): Array[String] = {
    // -1 in case after splitting the last column is empty, the surrogate key ahs to be generated
    // for empty value too
    patterns(delimiterIndex).split(input, -1)
  }

  def cloneAndIncreaseIndex: DataFormat = {
    DataFormat(delimiters, Math.min(delimiterIndex + 1, delimiters.length - 1), patterns)
  }
}

/**
 * a case class to package some attributes
 */
case class DictionaryLoadModel(table: CarbonTableIdentifier,
                               dimensions: Array[CarbonDimension],
                               hdfsLocation: String,
                               dictfolderPath: String,
                               dictFilePaths: Array[String],
                               dictFileExists: Array[Boolean],
                               isComplexes: Array[Boolean],
                               primDimensions: Array[CarbonDimension],
                               delimiters: Array[String]) extends Serializable
/**
 * A RDD to combine distinct values in block.
 *
 * @constructor create a RDD with RDD[Row]
 * @param prev the input RDD[Row]
 * @param model a model package load info
 */
class CarbonBlockDistinctValuesCombineRDD(
  prev: RDD[Row],
  model: DictionaryLoadModel)
    extends RDD[(Int, Array[String])](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

  override def compute(split: Partition, context: TaskContext
      ): Iterator[(Int, Array[String])] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());

    val distinctValuesList = new ArrayBuffer[(Int, HashSet[String])]
    try {
      // local combine set
      val dimNum = model.dimensions.length
      val primDimNum = model.primDimensions.length
      val columnValues = new Array[HashSet[String]](primDimNum)
      val mapColumnValuesWithId = new HashMap[String, HashSet[String]]
      for (i <- 0 until primDimNum) {
        columnValues(i) = new HashSet[String]
        distinctValuesList += ((i, columnValues(i)))
        mapColumnValuesWithId.put(model.primDimensions(i).getColumnId, columnValues(i))
      }
      val dimensionParsers = new Array[GenericParser](dimNum)
      for (j <- 0 until dimNum) {
        dimensionParsers(j) = GlobalDictionaryUtil.generateParserForDimension(
          Some(model.dimensions(j)),
          GlobalDictionaryUtil.createDataFormat(model.delimiters),
          mapColumnValuesWithId).get
      }
      var row: Row = null
      var value: String = null
      val rddIter = firstParent[Row].iterator(split, context)
      // generate block distinct value set
      while (rddIter.hasNext) {
        row = rddIter.next()
        if (row != null) {
          for (i <- 0 until dimNum) {
            dimensionParsers(i).parseString(row.getString(i))
          }
        }
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(ex)
    }
    distinctValuesList.map { iter =>
      val valueList = iter._2.toArray
      (iter._1, valueList)
    }.iterator
  }
}

/**
 * A RDD to generate dictionary file for each column
 *
 * @constructor create a RDD with RDD[Row]
 * @param prev the input RDD[Row]
 * @param model a model package load info
 */
class CarbonGlobalDictionaryGenerateRDD(
  prev: RDD[(Int, Array[String])],
  model: DictionaryLoadModel)
    extends RDD[(String, String)](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[(Int, Array[String])].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    var status = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    val iter = new Iterator[(String, String)] {
      // generate distinct value list
      try {
        val t1 = System.currentTimeMillis
        val dictionary = if (model.dictFileExists(split.index)) {
          CarbonLoaderUtil.getDictionary(model.table,
            model.primDimensions(split.index).getColumnId,
            model.hdfsLocation,
            model.primDimensions(split.index).getDataType
          )
        } else {
          null
        }
        val t2 = System.currentTimeMillis
        val valuesBuffer = new mutable.HashSet[String]
        val rddIter = firstParent[(Int, Array[String])].iterator(split, context)
        while (rddIter.hasNext) {
          valuesBuffer ++= rddIter.next()._2
        }
        val t3 = System.currentTimeMillis
        val distinctValueCount = GlobalDictionaryUtil.generateAndWriteNewDistinctValueList(
          valuesBuffer, dictionary, model, split.index
        )
        // clear the value buffer after writing dictionary data
        valuesBuffer.clear

        val t4 = System.currentTimeMillis

        if (distinctValueCount > 0) {
          val columnDictionary = CarbonLoaderUtil.getDictionary(model.table,
            model.primDimensions(split.index).getColumnId,
            model.hdfsLocation,
            model.primDimensions(split.index).getDataType)
          GlobalDictionaryUtil.writeGlobalDictionaryColumnSortInfo(model, split.index,
            columnDictionary
          )
          val t5 = System.currentTimeMillis

          LOGGER.info("\n columnName:" + model.primDimensions(split.index).getColName +
                        "\n columnId:" + model.primDimensions(split.index).getColumnId +
                        "\n new distinct values count:" + distinctValueCount +
                        "\n create dictionary cache:" + (t2 - t1) +
                        "\n combine lists:" + (t3 - t2) +
                        "\n sort list, distinct and write:" + (t4 - t3) +
                        "\n write sort info:" + (t5 - t4))
        }
      } catch {
        case ex: Exception =>
          status = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          LOGGER.error(ex)
      }
      var finished = false

      override def hasNext: Boolean = {

        if (!finished) {
          finished = true
          finished
        } else {
          !finished
        }
      }

      override def next(): (String, String) = {
        (model.primDimensions(split.index).getColName, status)
      }
    }
    iter
  }
}

/**
 *  Set column dictionry patition format
 * @param id
 * @param dimension
 * @param colPath
 * @param colIndex
 */
class CarbonColumnDictPatition(id: Int, dimension: CarbonDimension, colPath: String, colIndex: Int)
  extends Partition {
  override val index: Int = id
  val colDimension = dimension
  val columnPath = colPath
  // the start index for dimension columns to adapt for CarbonGlobalDictionaryGenerateRDD
  val colStartIndex = colIndex
}

/**
 * Use external column dict to generate global dictionary
 * @param carbonLoadModel
 * @param sparkContext
 * @param dictColumnPaths
 * @param table
 * @param dimensions
 * @param hdfsLocation
 * @param dictFolderPath
 */
class CarbonColumnDictGenerateRDD(carbonLoadModel: CarbonLoadModel,
                                  sparkContext: SparkContext,
                                  dictColumnPaths: Array[String],
                                  table: CarbonTableIdentifier,
                                  dimensions: Array[CarbonDimension],
                                  hdfsLocation: String,
                                  dictFolderPath: String)
  extends RDD[(Int, Array[String])](sparkContext, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val extColumnlen = dictColumnPaths.length
    val result = new Array[Partition](extColumnlen)
    var primColStarIndex = 0
    for (i <- 0 until extColumnlen) {
      if (i > 0) {
        val dimLen = GlobalDictionaryUtil.getPrimDimensionWithDict(dimensions(i-1)).length
        primColStarIndex += dimLen
      }
      result(i) = new CarbonColumnDictPatition(i, dimensions(i), dictColumnPaths(i),
        primColStarIndex)
    }
    result
  }

  override def compute(split: Partition, context: TaskContext)
  : Iterator[(Int, Array[String])] = {
    val theSplit = split.asInstanceOf[CarbonColumnDictPatition]
    // read the column dict data
    val inputStream = FileFactory.getDataInputStream(theSplit.columnPath,
      FileFactory.getFileType(theSplit.columnPath))
    val csvReader = new CSVReader(new InputStreamReader(inputStream, Charset.defaultCharset),
      CSVReader.DEFAULT_SKIP_LINES, new CSVParser())
    // read the column data to list
    val colDict = csvReader.readAll().iterator()
    val distinctValues = new ArrayBuffer[(Int, HashSet[String])]
    val dictModel = GlobalDictionaryUtil.
      createDictionaryLoadModel(carbonLoadModel, table, Array(theSplit.colDimension),
      hdfsLocation, dictFolderPath)
    val mapIdWithSet = new HashMap[String, HashSet[String]]
    val columnValues = new Array[HashSet[String]](dictModel.primDimensions.length)
    for (i <- 0 until dictModel.primDimensions.length) {
      columnValues(i) = new HashSet[String]
      distinctValues += ((i + theSplit.colStartIndex, columnValues(i)))
      mapIdWithSet.put(dictModel.primDimensions(i).getColumnId, columnValues(i))
    }
    // use parser to generate new dict value
    val dimensionParser = GlobalDictionaryUtil.generateParserForDimension(
      Some(theSplit.colDimension),
      createDataFormat(dictModel.delimiters),
      mapIdWithSet).get
    // parse the column data
    while (colDict.hasNext) {
      dimensionParser.parseString(colDict.next()(0))
    }
    distinctValues.map { iter =>
      (iter._1, iter._2.toArray)
    }.iterator
  }
}
