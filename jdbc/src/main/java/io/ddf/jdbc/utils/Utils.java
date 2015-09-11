package io.ddf.jdbc.utils;

import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

import java.sql.Types;

public class Utils {

  /**
   * Given a String[] vector of data values along one column, try to infer what the data type should be.
   * <p/>
   * TODO: precompile regex
   *
   * @param vector
   * @return string representing name of the type "integer", "double", "character", or "logical" The algorithm will
   * first scan the vector to detect whether the vector contains only digits, ',' and '.', <br>
   * if true, then it will detect whether the vector contains '.', <br>
   * &nbsp; &nbsp; if true then the vector is double else it is integer <br>
   * if false, then it will detect whether the vector contains only 'T' and 'F' <br>
   * &nbsp; &nbsp; if true then the vector is logical, otherwise it is characters
   */
  public static String determineType(String[] vector, Boolean doPreferDouble, boolean isForSchema) {
    boolean isNumber = true;
    boolean isInteger = true;
    boolean isLogical = true;
    boolean allNA = true;

    for (String s : vector) {
      if (s == null || s.startsWith("NA") || s.startsWith("Na") || s.matches("^\\s*$")) {
        // Ignore, don't set the type based on this
        continue;
      }

      allNA = false;

      if (isNumber) {
        // match numbers: 123,456.123 123 123,456 456.123 .123
        if (!s.matches("(^|^-)((\\d+(,\\d+)*)|(\\d*))\\.?\\d+$")) {
          isNumber = false;
        }
        // match double
        else if (isInteger && s.matches("(^|^-)\\d*\\.{1}\\d+$")) {
          isInteger = false;
        }
      }

      // NOTE: cannot use "else" because isNumber changed in the previous
      // if block
      if (isLogical && !s.toLowerCase().matches("^t|f|true|false$")) {
        isLogical = false;
      }
    }

    // String result = "Unknown";
    String result = "string";

    if (!allNA) {
      if (isNumber) {
        if (isInteger) {
          result = "int";
        } else if (doPreferDouble) {
          result = "double";
        } else {
          result = "float";
        }
      } else {
        if (isLogical) {
          result = isForSchema ? "boolean" : "bool";
        } else {
          result = "string";
        }
      }
    }
    return result;
  }

  /**
   *
   * @return DDF Column type
   */
  public static Schema.ColumnType getDDFType(Integer colType) throws DDFException {
    switch(colType) {
      case Types.ARRAY: return Schema.ColumnType.ARRAY;
      case Types.BIGINT:  return Schema.ColumnType.BIGINT;
      case Types.BINARY: return Schema.ColumnType.BINARY;
      case Types.BIT: return Schema.ColumnType.BOOLEAN; //TODO: verify
      case Types.BLOB: return Schema.ColumnType.BLOB;
      case Types.BOOLEAN: return Schema.ColumnType.BOOLEAN;
      case Types.CHAR: return Schema.ColumnType.STRING;
      case Types.CLOB:
      case Types.DATALINK:
      case Types.DATE: return Schema.ColumnType.DATE;
      case Types.DECIMAL: return Schema.ColumnType.DECIMAL;
      case Types.DISTINCT:
      case Types.DOUBLE: return Schema.ColumnType.DOUBLE;
      case Types.FLOAT: return Schema.ColumnType.FLOAT;
      case Types.INTEGER: return Schema.ColumnType.INT;
      case Types.LONGVARCHAR: return Schema.ColumnType.STRING; //TODO: verify
      case Types.NUMERIC: return Schema.ColumnType.DECIMAL;
      case Types.NVARCHAR: return Schema.ColumnType.STRING; //TODO: verify
      case Types.REAL: return Schema.ColumnType.FLOAT;
      case Types.SMALLINT: return Schema.ColumnType.INT;
      case Types.TIMESTAMP: return Schema.ColumnType.TIMESTAMP;
      case Types.TINYINT: return Schema.ColumnType.INT;
      case Types.VARCHAR: return Schema.ColumnType.STRING; //TODO: verify
      case Types.VARBINARY: return Schema.ColumnType.BINARY;
      default: throw new DDFException("Type not support " + colType);
        //TODO: complete for other types
    }
  }
}
