package io.ddf.aws.ml.util;


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
import java.util.ArrayList;
import java.util.List;

public class CrossValidation {
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

  private final float TRAIN = 0.7f;
  private final float TEST = 0.3f;

  public List<CrossValidationSet> CVRandom(int k, double trainingSize) throws DDFException {
    List<CrossValidationSet> finalDDFList = new ArrayList<>();
    if (trainingSize >= 1 || trainingSize <= 0)
      throw new DDFException("CVRandom cannot be performed with the training size provided");
    long resultSize = rowCount / k;

    for (int i = 0; i < k; i++) {
      String temp = Identifiers.newTableName("temp");
      String sqlTest = String
          .format("CREATE TABLE %s AS SELECT * FROM %s ORDER BY RANDOM() LIMIT ?", temp + "test", ddf.getTableName());
      String sqlTrain = String
          .format("CREATE TABLE %s AS SELECT * FROM %s ORDER BY RANDOM() LIMIT ?", temp + "train", ddf.getTableName());
      executeDDL(sqlTest, (long) (resultSize * (1 - trainingSize)), -1);
      executeDDL(sqlTrain, (long) (resultSize * trainingSize), -1);

      DDF trainDDF = create(temp + "train");
      DDF testDDF = create(temp + "test");
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

  public List<CrossValidationSet> CVK(int k) {
    List<CrossValidationSet> finalDDFList = new ArrayList<>();
    long resultSize = rowCount / k;
    for (int i = 0; i < k; i++) {
      String temp = Identifiers.newTableName("temp");
      String sqlTest = String
          .format("CREATE TABLE %s AS SELECT * FROM %s LIMIT ? OFFSET ?", temp + "test", ddf.getTableName());
      String sqlTrain = String
          .format("CREATE TABLE %s AS SELECT * FROM %s LIMIT ? OFFSET ?", temp + "train", ddf.getTableName());
      executeDDL(sqlTest, (long) (resultSize * TEST), i * resultSize);
      executeDDL(sqlTrain, (long) (resultSize * TRAIN), (long) ((i + TEST) * resultSize));
      DDF trainDDF = create(temp + "train");
      DDF testDDF = create(temp + "test");
      finalDDFList.add(new CrossValidationSet(trainDDF, testDDF));
    }
    return finalDDFList;
  }

  public void executeDDL(String ddlString, long resultSize, long offset) {
    try (Connection conn = awsddfManager.getConnection()) {
      try (PreparedStatement stmt = conn.prepareStatement(ddlString)) {
        stmt.setInt(1, (int) resultSize);
        if (offset >= 0) stmt.setInt(2, (int) offset);
        stmt.executeUpdate();
      } catch (SQLException exception) {
        throw new RuntimeException(exception);
      }
    } catch (SQLException exception) {
      throw new RuntimeException(exception);
    }
  }

}

