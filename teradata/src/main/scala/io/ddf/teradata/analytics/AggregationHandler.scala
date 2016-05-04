package io.ddf.teradata.analytics

import java.util
import scala.collection.JavaConversions._
import com.google.common.base.{Joiner, Strings}
import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.datasource.SQLDataSourceDescriptor
import io.ddf.exception.DDFException
import io.ddf.types.AggregateTypes
import io.ddf.types.AggregateTypes.{AggregateFunction, AggregationResult,
AggregateField}
import org.apache.commons.lang.StringUtils

class AggregationHandler(ddf: DDF)
  extends io.ddf.jdbc.analytics.AggregationHandler(ddf) {
  private var mGroupedColumns: util.List[String] = null

  /**
    * this function is overridden to take care of the stddev aggregate function
    * in Teradata which differs from the one used in ddf-core
    */
  override def aggregate(fields: util.List[AggregateField]):
  AggregationResult = {
    val tableName: String = this.getDDF.getTableName
    if (fields == null || fields.isEmpty) {
      throw new DDFException(new UnsupportedOperationException("Field array" +
        " cannot be null or empty"))
    }
    if (Strings.isNullOrEmpty(tableName)) {
      throw new DDFException("Table name cannot be null or empty")
    }
    val sqlCmd: String = String.format(
      "SELECT %s FROM %s GROUP BY %s",
      toSqlSpecs(fields, true),
      tableName,
      toSqlSpecs(fields, false))
    mLog.info("SQL Command: " + sqlCmd)
    val numUnaggregatedFields: Int = fields.count(field => !field.isAggregated)
    try {
      val result: util.List[String] = this.getManager.sql(sqlCmd, false).getRows
      AggregationResult.newInstance(result, numUnaggregatedFields)
    }
    catch {
      case e: Exception => mLog.error(e.getMessage)
        throw new DDFException("Unable to query from " + tableName, e)
    }
  }

  /** This function takes care of the stddev function in teradata
    * @param fields The AggregateFields representing a list of column specs,
    *               some of which may be aggregated, while other
    *               non-aggregated fields are the GROUP BY keys
    * @param isFieldSpecs If true, include all fields. If false, include only
    *                     unaggregated fields.
    * @return The string of sql statement combining the aggregate fields
    */
  private def toSqlSpecs(fields: util.List[AggregateTypes.AggregateField],
                         isFieldSpecs: Boolean): String = {
    val specs: util.List[String] = Lists.newArrayList()
    import scala.collection.JavaConversions._
    for (field <- fields if isFieldSpecs || !field.isAggregated) {
      field.getAggregateFunction match {
        case AggregateFunction.STDDEV =>
          val func: String = String.format("STDDEV_SAMP(cast(%s as float))",
            field.getColumn)
          val fieldName =
            if (Strings.isNullOrEmpty(field.mName))
              func
            else
              String.format("%s AS %s", func, field.mName)
          specs.add(fieldName)
        case _ => specs.add(field.toString)
      }
    }
    StringUtils.join(specs.toArray(), ',')
  }

  /**
    * The groupBy function has been overridden to set the mGroupedColumns
    * variable, used by the agg function.
    */
  @throws(classOf[DDFException])
  override def groupBy(groupedColumns: util.List[String],
                       aggregateFunctions: util.List[String]): DDF = {
    mGroupedColumns = groupedColumns
    agg(aggregateFunctions)
  }

  @throws(classOf[DDFException])
  override def groupBy(groupedColumns: util.List[String]): DDF = {
    mGroupedColumns = groupedColumns
    this.getDDF
  }

  /**
    * This function is overridden to ensure that the proper format for stddev
    * for Teradata is used by changing the convertAggregateFunctionsToSql
    * function
    */
  @throws(classOf[DDFException])
  override def agg(aggregateFunctions: util.List[String]): DDF = {
    if (mGroupedColumns.size > 0) {
      val groupedColSql: String = Joiner.on(",").join(mGroupedColumns)
      val selectFuncSql: String = {
        for (i <- aggregateFunctions.indices) yield {
          convertAggregateFunctionsToSql(aggregateFunctions.get(i))
        }
      }.mkString(",")
      val sqlCmd: String = String.format(
        "SELECT %s , %s FROM %s GROUP BY %s",
        selectFuncSql,
        groupedColSql,
        "{1}",
        groupedColSql)
      mLog.info("SQL Command: " + sqlCmd)
      try {
        val resultDDF: DDF = this.getManager.sql2ddf(
          sqlCmd,
          new SQLDataSourceDescriptor(
            sqlCmd,
            true,
            null,
            null,
            this.getDDF.getUUID.toString))
        resultDDF
      }
      catch {
        case e: Exception => throw new DDFException("Unable to query from " +
            this.getDDF.getTableName, e)
      }
    }
    else {
      throw new DDFException("Need to set grouped columns before aggregation")
    }
  }

  /**
    * This function is changed from the ddf-core implementation to call
    * toSqlSpecs function which replaces stddev function with the Teradata
    * equivalent.
    */
  private def convertAggregateFunctionsToSql(sql: String): String = {
    if (Strings.isNullOrEmpty(sql)) return null
    val splits: Array[String] = sql.trim.split("=(?![^()]*+\\))")
    val specs: util.List[AggregateField] = Lists.newArrayList()
    splits.length match {
      case 2 => specs.add(AggregateField.fromFieldSpec(splits(1)))
        String.format("%s AS %s", toSqlSpecs(specs, true), splits(0))
      case 1 => specs.add(AggregateField.fromFieldSpec(splits(0)))
        toSqlSpecs(specs, true)
      case _ => sql
    }
  }
}

