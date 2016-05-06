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

package org.carbondata.integration.spark.util

import java.io.IOException
import java.util.ArrayList
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.language.implicitConversions
import scala.util.control.Breaks.{break, breakable}

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{DataFrame, SQLContext}

import org.carbondata.core.cache.dictionary.Dictionary
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.carbon.path.CarbonStorePath
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.reader.{CarbonDictionaryReader, CarbonDictionaryReaderImpl}
import org.carbondata.core.util.ByteUtil.UnsafeComparer
import org.carbondata.core.util.CarbonUtil
import org.carbondata.core.writer.{CarbonDictionaryWriter, CarbonDictionaryWriterImpl}
import org.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriter, CarbonDictionarySortIndexWriterImpl}
import org.carbondata.integration.spark.load.CarbonDictionarySortInfo
import org.carbondata.integration.spark.load.CarbonDictionarySortInfoPreparator
import org.carbondata.integration.spark.load.CarbonLoaderUtil
import org.carbondata.integration.spark.load.CarbonLoadModel
import org.carbondata.integration.spark.partition.reader.CSVWriter
import org.carbondata.integration.spark.rdd._

/**
 * A object which provide a method to generate global dictionary from CSV files.
 */
object GlobalDictionaryUtil extends Logging {

  /**
   * find columns which need to generate global dictionary.
   * @param dimensions
   * @param headers
   * @param columns
   * @param extDimensions
   * @return
   */
  def pruneDimensions(dimensions: Array[CarbonDimension], headers: Array[String],
                      columns: Array[String], extDimensions: Option[Array[CarbonDimension]]
  = None): (Array[CarbonDimension], Array[String]) = {
    val dimensionBuffer = new ArrayBuffer[CarbonDimension]
    val columnNameBuffer = new ArrayBuffer[String]
    val dimensionsWithDict = dimensions.filter(hasEncoding(_, Encoding.DICTIONARY,
      Encoding.DIRECT_DICTIONARY))
    var dimName: String = null
    for (dim <- dimensionsWithDict) {
      breakable {
        for (i <- 0 until headers.length) {
          dimName = dim.getColName
          if (dimName.equalsIgnoreCase(headers(i)) && (extDimensions.isEmpty ||
            !extDimensions.get.exists(_.getColName.equalsIgnoreCase(dimName)))) {
            dimensionBuffer += dim
            columnNameBuffer += columns(i)
            break
          }
        }
      }
    }
    (dimensionBuffer.toArray, columnNameBuffer.toArray)
  }

  /**
   *  use this method to judge whether CarbonDimension use some encoding or not
   * @param dimension
   * @param encoding
   * @param excludeEncoding
   * @return
   */
  def hasEncoding(dimension: CarbonDimension, encoding: Encoding,
    excludeEncoding: Encoding): Boolean = {
    if (dimension.isComplex()) {
      var has = false
      var children = dimension.getListOfChildDimensions
      breakable {
        for (i <- 0 until children.size) {
          has = has || hasEncoding(children.get(i), encoding, excludeEncoding);
          if (has) {
            break;
          }
        }
      }
      has
    } else {
      dimension.hasEncoding(encoding) &&
        ( excludeEncoding == null || ! dimension.hasEncoding(excludeEncoding))
    }
  }

  def gatherDimensionByEncoding(dimension: CarbonDimension,
                                encoding: Encoding,
                                excludeEncoding: Encoding,
                                dimensionsWithEncoding: ArrayBuffer[CarbonDimension]) {
    if (dimension.isComplex()) {
      val children = dimension.getListOfChildDimensions()
      for (i <- 0 until children.size) {
        gatherDimensionByEncoding(children.get(i), encoding, excludeEncoding,
          dimensionsWithEncoding)
      }
    } else {
      if (dimension.hasEncoding(encoding) &&
        ( excludeEncoding == null || ! dimension.hasEncoding(excludeEncoding))) {
        dimensionsWithEncoding += dimension
      }
    }
  }

  def getPrimDimensionWithDict(dimension: CarbonDimension): Array[CarbonDimension] = {
    val dimensionsWithDict = new ArrayBuffer[CarbonDimension]
    gatherDimensionByEncoding(dimension, Encoding.DICTIONARY, Encoding.DIRECT_DICTIONARY,
      dimensionsWithDict)
    dimensionsWithDict.toArray
  }

  /**
   * invoke CarbonDictionaryWriter to write dictionary to file.
   *
   * @param model instance of DictionaryLoadModel
   * @param columnIndex the index of current column in column list
   * @param iter distinct value list of dictionary
   */
  def writeGlobalDictionaryToFile(model: DictionaryLoadModel,
                                  columnIndex: Int,
                                  iter: Iterator[String]): Unit = {
    val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(
      model.hdfsLocation, model.table,
      model.primDimensions(columnIndex).getColumnId)
    try {
      while (iter.hasNext) {
        writer.write(iter.next)
      }
    } finally {
      writer.close
    }
  }

  /**
   * invokes the CarbonDictionarySortIndexWriter to write column sort info
   * sortIndex and sortIndexInverted data to sortinsex file.
   *
   * @param model
   * @param index
   */
  def writeGlobalDictionaryColumnSortInfo(model: DictionaryLoadModel, index: Int,
    dictionary: Dictionary): Unit = {
    val preparator: CarbonDictionarySortInfoPreparator =
      new CarbonDictionarySortInfoPreparator(model.hdfsLocation, model.table)
    val dictionarySortInfo: CarbonDictionarySortInfo =
      preparator.getDictionarySortInfo(dictionary,
        model.primDimensions(index).getDataType)
    val carbonDictionaryWriter: CarbonDictionarySortIndexWriter =
      new CarbonDictionarySortIndexWriterImpl(model.table,
        model.primDimensions(index).getColumnId, model.hdfsLocation)
    try {
      carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex)
      carbonDictionaryWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted)
    } finally {
      carbonDictionaryWriter.close()
    }
  }

  /**
   * read global dictionary from cache
   */
  def readGlobalDictionaryFromCache(model: DictionaryLoadModel
  ): HashMap[String, Dictionary] = {
    val dictMap = new HashMap[String, Dictionary]
    for (i <- 0 until model.primDimensions.length) {
      if (model.dictFileExists(i)) {
        val dict = CarbonLoaderUtil.getDictionary(model.table,
          model.primDimensions(i).getColumnId, model.hdfsLocation,
          model.primDimensions(i).getDataType
        )
        dictMap.put(model.primDimensions(i).getColumnId, dict)
      }
    }
    dictMap
  }

  /**
   * invoke CarbonDictionaryReader to read dictionary from files.
   * @param model
   * @return
   */
  def readGlobalDictionaryFromFile(model: DictionaryLoadModel
  ): HashMap[String, HashSet[String]] = {
    val dictMap = new HashMap[String, HashSet[String]]
    for (i <- 0 until model.primDimensions.length) {
      val set = new HashSet[String]
      if (model.dictFileExists(i)) {
        val reader: CarbonDictionaryReader = new CarbonDictionaryReaderImpl(
          model.hdfsLocation, model.table, model.primDimensions(i).getColumnId)
        val values = reader.read
        if (values != null) {
          for (j <- 0 until values.size)
            set.add(new String(values.get(j)))
        }
      }
      dictMap.put(model.primDimensions(i).getColumnId, set)
    }
    dictMap
  }

  def generateParserForChildrenDimension(dim: CarbonDimension,
                                         format: DataFormat,
                                         mapColumnValuesWithId:
                                           HashMap[String, HashSet[Array[Byte]]],
                                         generic: GenericParser): Unit = {
    val children = dim.getListOfChildDimensions.asScala
    for (i <- 0 until children.length) {
      generateParserForDimension(Some(children(i)), format.cloneAndIncreaseIndex,
        mapColumnValuesWithId) match {
          case Some(childDim) =>
            generic.addChild(childDim)
          case None =>
        }
    }
  }

  def generateParserForDimension(dimension: Option[CarbonDimension],
                                 format: DataFormat,
                                 mapColumnValuesWithId: HashMap[String, HashSet[Array[Byte]]]
  ): Option[GenericParser] = {
    dimension match {
      case None =>
        None
      case Some(dim) =>
        dim.getDataType match {
          case DataType.ARRAY =>
            val arrDim = ArrayParser(dim, format)
            generateParserForChildrenDimension(dim, format, mapColumnValuesWithId, arrDim)
            Some(arrDim)
          case DataType.STRUCT =>
            val stuDim = StructParser(dim, format)
            generateParserForChildrenDimension(dim, format, mapColumnValuesWithId, stuDim)
            Some(stuDim)
          case _ =>
            Some(PrimitiveParser(dim, mapColumnValuesWithId.get(dim.getColumnId)))
        }
    }
  }

  def createDataFormat(delimiters: Array[String]): DataFormat = {
    if (ArrayUtils.isNotEmpty(delimiters)) {
      val patterns = new Array[Pattern](delimiters.length)
      for (i <- 0 until patterns.length) {
        patterns(i) = Pattern.compile(if (delimiters(i)== null) "" else delimiters(i))
      }
      DataFormat(delimiters, 0, patterns)
    } else {
      null
    }
  }

  /**
   * create a instance of DictionaryLoadModel
   *
   * @param carbonLoadModel
   * @param table
   * @param dimensions
   * @param hdfsLocation
   * @param dictfolderPath
   * @return org.carbondata.integration.spark.rdd.DictionaryLoadModel
   */
  def createDictionaryLoadModel(carbonLoadModel: CarbonLoadModel,
                                table: CarbonTableIdentifier,
                                dimensions: Array[CarbonDimension],
                                hdfsLocation: String,
                                dictfolderPath: String): DictionaryLoadModel = {
    val primDimensionsBuffer = new ArrayBuffer[CarbonDimension]
    for (i <- 0 until dimensions.length) {
      val dims = getPrimDimensionWithDict(dimensions(i))
      for (j <- 0 until dims.length) {
        primDimensionsBuffer += dims(j)
      }
    }
    val primDimensions = primDimensionsBuffer.toSeq.map { x => x }.toArray
    // list dictionary file path
    val dictFilePaths = new Array[String](primDimensions.length)
    val dictFileExists = new Array[Boolean](primDimensions.length)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(hdfsLocation, table)
    for (i <- 0 until primDimensions.length) {
      dictFilePaths(i) = carbonTablePath.getDictionaryFilePath(primDimensions(i).getColumnId)
      dictFileExists(i) = CarbonUtil.isFileExists(dictFilePaths(i))
    }
    new DictionaryLoadModel(table,
      dimensions,
      hdfsLocation,
      dictfolderPath,
      dictFilePaths,
      dictFileExists,
      dimensions.map(_.isComplex() == true),
      primDimensions,
      carbonLoadModel.getDelimiters)
  }

  /**
   * append all file path to a String, file path separated by comma
   */
  def getCsvRecursivePathsFromCarbonFile(carbonFile: CarbonFile): String = {
    if (carbonFile.isDirectory()) {
      val files = carbonFile.listFiles()
      val stringbuild = new StringBuilder()
      for (j <- 0 until files.size) {
        stringbuild.append(getCsvRecursivePathsFromCarbonFile(files(j))).append(",")
      }
      stringbuild.substring(0, stringbuild.size - 1)
    } else {
      val path = carbonFile.getPath
      if ("csv".equalsIgnoreCase(path.substring(path.length - 3))) path else ""
    }
  }

  /**
   * append all file path to a String, inputPath path separated by comma
   */
  def getCsvRecursivePaths(inputPath: String): String = {
    if (inputPath == null || inputPath.isEmpty) {
      inputPath
    } else {
      val stringbuild = new StringBuilder()
      val filePaths = inputPath.split(",")
      for (i <- 0 until filePaths.size) {
        val fileType = FileFactory.getFileType(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), fileType)
        stringbuild.append(getCsvRecursivePathsFromCarbonFile(carbonFile)).append(",")
      }
      stringbuild.substring(0, stringbuild.size - 1)
    }
  }

  /**
   * load CSV files to DataFrame by using datasource "com.databricks.spark.csv"
   * @param sqlContext
   * @param carbonLoadModel
   * @return org.apache.spark.sql.DataFrame
   */
  private def loadDataFrame(sqlContext: SQLContext,
                            carbonLoadModel: CarbonLoadModel): DataFrame = {
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header",
        {if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) "true"
          else "false" })
      .option("delimiter",
        {if (StringUtils.isEmpty(carbonLoadModel.getCsvDelimiter))"" + CSVWriter.DEFAULT_SEPARATOR
          else carbonLoadModel.getCsvDelimiter})
      .option("parserLib", "univocity")
      .load(getCsvRecursivePaths(carbonLoadModel.getFactFilePath))
    df
  }

  /**
   * check whether global dictionary have been generated successfully or not
   * @param status
   */
  private def checkStatus(status: Array[(String, String)]) = {
    if (status.exists(x => CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(x._2))) {
      logError("generate global dictionary files failed")
      throw new Exception("Failed to generate global dictionary files")
    } else {
      logInfo("generate global dictionary successfully")
    }
  }

  /**
   *  get external column dictionary file path
   * @param colDictFilePath
   * @param table
   * @param dimensions
   * @return
   */
  private def getExternalColumnDictPath(colDictFilePath: String,
                                        table: CarbonTableIdentifier,
                                        dimensions: Array[CarbonDimension]) = {
    val dictCarbonDimensions = ArrayBuffer[CarbonDimension]()
    val dictColumnPaths = ArrayBuffer[String]()
    val dataBaseName = table.getDatabaseName
    val tableName = table.getTableName
    val colFileMapArray = colDictFilePath.split(",")
    for (colPathMap <- colFileMapArray) {
      val colPathMapTrim = colPathMap.trim
      val colNameWithPath = colPathMapTrim.split(":")
      if (colNameWithPath.length == 1) {
        logError("the format of external column dictionary should be " +
          "columnName:columnPath, please check")
        throw new IllegalArgumentException
      }
      val colName = colNameWithPath(0)
      // judge whether the column is exists
      if (!dimensions.exists(_.getColName.equalsIgnoreCase(colName))) {
        logError(s"No column $colName exists in $dataBaseName.$tableName")
        throw new IllegalArgumentException
      }
      breakable {
        dimensions.foreach(x =>
          if (x.getColName.equalsIgnoreCase(colName)) {
            dictCarbonDimensions += x
            dictColumnPaths += colPathMapTrim.substring(colName.length + 1)
            break
          })
      }
    }
    (dictCarbonDimensions.toArray, dictColumnPaths.toArray)
  }

  /**
   *  use external dimension column to generate global dictionary
   * @param colDictFilePath
   * @param table
   * @param dimensions
   * @param carbonLoadModel
   * @param sparkContext
   * @param hdfsLocation
   * @param dictFolderPath
   * @return
   */
  private def generateExternalColumnDictionary(colDictFilePath: String,
                                               table: CarbonTableIdentifier,
                                               dimensions: Array[CarbonDimension],
                                               carbonLoadModel: CarbonLoadModel,
                                               sparkContext: SparkContext,
                                               hdfsLocation: String,
                                               dictFolderPath: String) = {
    // get external dictionary column
    val extColumns = getExternalColumnDictPath(colDictFilePath, table, dimensions)
    val dictmodel = createDictionaryLoadModel(carbonLoadModel, table, extColumns._1,
      hdfsLocation, dictFolderPath)
    // new RDD to achieve distributed column dict generation
    val extInputRDD = new CarbonColumnDictGenerateRDD(carbonLoadModel, sparkContext,
      extColumns._2, table, extColumns._1, hdfsLocation, dictFolderPath)
      .partitionBy(new ColumnPartitioner(dictmodel.primDimensions.length))
    val statusList = new CarbonGlobalDictionaryGenerateRDD(extInputRDD, dictmodel).collect()
    // check result status
    checkStatus(statusList)
    extColumns._1
  }

  /**
   * generate global dictionary with SQLContext and CarbonLoadModel
   * @param sqlContext
   * @param carbonLoadModel
   */
  def generateGlobalDictionary(sqlContext: SQLContext,
                               carbonLoadModel: CarbonLoadModel,
                               hdfsLocation: String): Unit = {
    try {
      val table = new CarbonTableIdentifier(carbonLoadModel.getDatabaseName,
        carbonLoadModel.getTableName)
      // get carbon dimension list
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val dimensions = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
        .asScala.toArray
      // create dictionary folder if not exists
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(hdfsLocation, table)
      val dictfolderPath = carbonTablePath.getMetadataDirectoryPath
      val created = CarbonUtil.checkAndCreateFolder(dictfolderPath)
      if (!created) {
        logError("Dictionary Folder creation status :: " + created)
        throw new IOException("Failed to created dictionary folder")
      }
      // load data by using dataSource com.databricks.spark.csv
      var df = loadDataFrame(sqlContext, carbonLoadModel)
      val headers = if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) df.columns
      else carbonLoadModel.getCsvHeader.split("" + CSVWriter.DEFAULT_SEPARATOR)
      // generate global dict from external column dict file
      val colDictFilePath = carbonLoadModel.getColDictFilePath
      var extDimensions: Option[Array[CarbonDimension]] = None
      if (colDictFilePath != null) {
        // get the dimensions that have been used in external column dictionary
        extDimensions = Some(generateExternalColumnDictionary(colDictFilePath, table,
          dimensions, carbonLoadModel, sqlContext.sparkContext, hdfsLocation, dictfolderPath))
      }
      // use fact file to generate global dict
      val (requireDimension, requireColumnNames) = pruneDimensions(dimensions,
        headers, df.columns, extDimensions)
      if (requireDimension.size >= 1) {
        // select column to push down pruning
        df = df.select(requireColumnNames.head, requireColumnNames.tail: _*)
        val model = createDictionaryLoadModel(carbonLoadModel, table, requireDimension,
          hdfsLocation, dictfolderPath)
        // combine distinct value in a block and partition by column
        val inputRDD = new CarbonBlockDistinctValuesCombineRDD(df.rdd, model)
          .partitionBy(new ColumnPartitioner(model.primDimensions.length))
        // generate global dictionary files
        val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, model).collect()
        // check result status
        checkStatus(statusList)
      } else {
        logInfo("have no column need to generate global dictionary in Fact file")
      }
      // generate global dict from dimension file
      if (carbonLoadModel.getDimFolderPath != null) {
        val fileMapArray = carbonLoadModel.getDimFolderPath.split(",")
        for (fileMap <- fileMapArray) {
          val dimTableName = fileMap.split(":")(0)
          val dimFilePath = fileMap.substring(dimTableName.length + 1)
          var dimDataframe = loadDataFrame(sqlContext, carbonLoadModel)
          val (requireDimensionForDim, requireColumnNamesForDim) =
            pruneDimensions(dimensions, dimDataframe.columns, dimDataframe.columns)
          if (requireDimensionForDim.size >= 1) {
            dimDataframe = dimDataframe.select(requireColumnNamesForDim.head,
              requireColumnNamesForDim.tail: _*)
            val modelforDim = createDictionaryLoadModel(carbonLoadModel, table,
              requireDimensionForDim, hdfsLocation, dictfolderPath)
            val inputRDDforDim = new CarbonBlockDistinctValuesCombineRDD(
              dimDataframe.rdd, modelforDim)
              .partitionBy(new ColumnPartitioner(modelforDim.primDimensions.length))
            val statusListforDim = new CarbonGlobalDictionaryGenerateRDD(
              inputRDDforDim, modelforDim).collect()
            checkStatus(statusListforDim)
          } else {
            logInfo(s"No columns in dimension table $dimTableName to generate global dictionary")
          }
        }
      }
    } catch {
      case ex: Exception =>
        logError("generate global dictionary failed")
        throw ex
    }
  }

  def generateAndWriteNewDistinctValueList(valuesBuffer: ArrayBuffer[Array[Byte]],
    dictionary: Dictionary,
    model: DictionaryLoadModel, columnIndex: Int): Int = {
    val values = valuesBuffer.toArray
    java.util.Arrays.sort(values, new ByteArrayComparator)
    var distinctValueCount: Int = 0
    val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(
      model.hdfsLocation, model.table,
      model.primDimensions(columnIndex).getColumnId)
    try {
      if (!model.dictFileExists(columnIndex)) {
        writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
        distinctValueCount += 1
      }
      if (values.length >= 1) {
        var preValue = values(0)
        if (model.dictFileExists(columnIndex)) {
          if (dictionary.getSurrogateKey(values(0)) == CarbonCommonConstants
            .INVALID_SURROGATE_KEY) {
            writer.write(values(0))
            distinctValueCount += 1
          }
          for (i <- 1 until values.length) {
            if (UnsafeComparer.INSTANCE.compareTo(values(i), preValue) != 0) {
              if (dictionary.getSurrogateKey(values(i)) ==
                CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                writer.write(values(i))
                preValue = values(i)
                distinctValueCount += 1
              }
            }
          }

        } else {
          writer.write(values(0))
          distinctValueCount += 1
          for (i <- 1 until values.length) {
            if (UnsafeComparer.INSTANCE.compareTo(values(i), preValue) != 0) {
              writer.write(values(i))
              preValue = values(i)
              distinctValueCount += 1
            }
          }
        }
      }
    } finally {
      writer.close
    }
    distinctValueCount
  }
}
