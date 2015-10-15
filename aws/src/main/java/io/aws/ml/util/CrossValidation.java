package io.aws.ml.util;


import io.ddf.DDF;
import io.ddf.aws.AWSDDFManager;
import io.ddf.aws.ml.Identifiers;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.content.TableNameRepresentation;
import io.ddf.ml.CrossValidationSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class CrossValidation {
  private Date date = new Date();
  private DDF ddf;
  private long rowCount;
  private AWSDDFManager awsddfManager;

  public CrossValidation(DDF ddf) {
    this.ddf = ddf;
    this.awsddfManager = (AWSDDFManager) ddf.getManager();
    try {
      rowCount = ddf.getNumRows();
    } catch (DDFException exception) {
      throw new RuntimeException(exception);
    }
  }

  private String SQL = "CREATE TABLE ? AS SELECT * FROM ? ORDER BY RANDOM() LIMIT ?";
  private String SQL_CVK = "CREATE TABLE ? AS SELECT * FROM ? LIMIT ? OFFSET ?";
  private float TRAIN = 0.7f;
  private float TEST = 0.3f;

  public List<CrossValidationSet> CVRandom(int k, double trainingSize, long seed) throws DDFException {
    List<CrossValidationSet> finalDDFList = new ArrayList<CrossValidationSet>();
    long resultSize = rowCount / ((int) trainingSize);
    for (int i = 0; i < k; i++) {
      String tempView = "temp" + date.getTime() + i;
      executeDDL(SQL, tempView + "test", (long) (resultSize * TEST), -1);
      executeDDL(SQL, tempView + "train", (long) (resultSize * TRAIN), -1);
      DDF trainDDF = create(tempView + "train");
      DDF testDDF = create(tempView + "test");
      finalDDFList.add(new CrossValidationSet(trainDDF, testDDF));
    }
    return finalDDFList;
  }

  public DDF create(String table) {
    Schema tableSchema = ddf.getSchema();
    TableNameRepresentation emptyRep = new TableNameRepresentation(table, tableSchema);
    try {
      return awsddfManager
          .newDDF(awsddfManager, emptyRep, Identifiers.representation(), ddf.getNamespace(), table, tableSchema);
    } catch (DDFException exception) {
      throw new RuntimeException(exception);
    }
  }

  public List<CrossValidationSet> CVK(int k, long seed) {
    List<CrossValidationSet> finalDDFList = new ArrayList<>();
    long resultSize = rowCount / k;
    for (int i = 0; i < k; i++) {
      String tempView = "temp" + date.getTime() + i;
      executeDDL(SQL_CVK, tempView + "test", (long) (resultSize * TEST), i * resultSize);
      executeDDL(SQL_CVK, tempView + "train", (long) (resultSize * TRAIN), (long) ((i + TEST) * resultSize));
      DDF trainDDF = create(tempView + "train");
      DDF testDDF = create(tempView + "test");
      finalDDFList.add(new CrossValidationSet(trainDDF, testDDF));
    }
    return finalDDFList;
  }

  public void executeDDL(String ddlString, String temp, long resultSize, long offset) {
    try (Connection conn = awsddfManager.getConnection();) {
      try (PreparedStatement stmt = conn.prepareStatement(ddlString);) {
        stmt.setString(1, temp);
        stmt.setString(2, ddf.getTableName());
        stmt.setLong(3, resultSize);
        if (offset > 0) stmt.setLong(4, offset);
        stmt.executeUpdate();
      } catch (SQLException exception) {
        throw new RuntimeException(exception);
      }
    } catch (SQLException exception) {
      throw new RuntimeException(exception);
    }
  }


  public Map<String, String> getSystemPropertiesAsMap() {
    Map<String, String> config = new HashMap<>();
    Properties systemProperties = System.getProperties();
    for (Map.Entry<Object, Object> x : systemProperties.entrySet()) {
      config.put((String) x.getKey(), (String) x.getValue());
    }
    return config;
  }

}
