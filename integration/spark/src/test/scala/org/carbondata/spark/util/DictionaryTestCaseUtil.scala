package org.carbondata.spark.util

import org.apache.spark.sql.CarbonRelation
import org.apache.spark.sql.common.util.CarbonHiveContext

import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.spark.load.CarbonLoaderUtil

/**
 * Utility for global dictionary test cases
 */
object DictionaryTestCaseUtil {

  /**
   *  check whether the dictionary of specified column generated
   * @param relation  carbon table relation
   * @param columnName  name of specified column
   * @param value  a value of column
   */
  def checkDictionary(relation: CarbonRelation, columnName: String, value: String) {
    val table = relation.cubeMeta.carbonTable
    val dimension = table.getDimensionByName(table.getFactTableName, columnName)
    val tableIdentifier = new CarbonTableIdentifier(table.getDatabaseName, table.getFactTableName, "uniqueid")

    val columnIdentifier = new DictionaryColumnUniqueIdentifier(tableIdentifier,
      dimension.getColumnIdentifier, dimension.getDataType
    )
    val dict = CarbonLoaderUtil.getDictionary(columnIdentifier,
      CarbonHiveContext.hdfsCarbonBasePath
    )
    assert(dict.getSurrogateKey(value) != CarbonCommonConstants.INVALID_SURROGATE_KEY)
  }

}
