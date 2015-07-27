package io.ddf.jdbc.utils;


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

}
