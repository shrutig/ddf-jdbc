package io.ddf.jdbc;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.Factor;
import io.ddf.content.IHandleRepresentations;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcDDF extends DDF {

  public JdbcDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {
    super(manager, data, typeSpecs, namespace, name, schema);
  }

  public JdbcDDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
    super(manager, defaultManagerIfNull);
  }


  public JdbcDDF(DDFManager manager) throws DDFException {
    super(manager);
  }

  @Override protected void initialize(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace,
      String name, Schema schema) throws DDFException {
    super.initialize(manager, data, typeSpecs, namespace, name, schema);
  }

}
