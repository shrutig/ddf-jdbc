package io.ddf.teradata;

import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.JdbcDDF;
import io.ddf.misc.Config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TeradataDDF extends JdbcDDF {
  public TeradataDDF(DDFManager manager,
                     Object data,
                     Class<?>[] typeSpecs,
                     String name,
                     Schema schema)
    throws DDFException {
    super(manager, data, typeSpecs, name, schema);
  }

  public TeradataDDF(DDFManager manager, DDFManager defaultManagerIfNull)
    throws DDFException {
    super(manager, defaultManagerIfNull);
  }


  public TeradataDDF(DDFManager manager) throws DDFException {
    super(manager);
  }

  /**
   * This function has been modified so that it returns a database.tablename
   * type of result for tablename for a teradata table as teradata throws a
   * table not found exception otherwise
   * @return The tablename in db.table format
   */
  @Override
  public String getTableName() {
    if(super.getTableName() == null)
      return null;
    String name = super.getTableName();
    Pattern p = Pattern.compile("^[a-zA-Z0-9_-]*[.][a-zA-Z0-9_-]*$");
    Matcher m = p.matcher(name);
    if (name.contains("[.]"))  {
      return name;
    } else {
      return ((TeradataDDFManager)this.getManager()).db()+ "." + name;
    }
  }
}
