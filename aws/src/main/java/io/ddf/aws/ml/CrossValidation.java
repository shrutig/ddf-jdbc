package io.ddf.aws.ml;

import io.ddf.DDF;
import io.ddf.aws.AWSDDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.JdbcDDF;
import io.ddf.jdbc.content.Representations;
import io.ddf.jdbc.content.Representations$;
import io.ddf.jdbc.content.TableNameRepresentation;
import io.ddf.jdbc.etl.SqlHandler;
import io.ddf.misc.Config;
import io.ddf.ml.CrossValidationSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CrossValidation {
    private Date date = new Date();
    private DDF ddf;
    private long rowCount;
    private AWSDDFManager awsddfManager;

    CrossValidation(DDF ddf) {
        this.ddf = ddf;
        this.awsddfManager = (AWSDDFManager) ddf.getManager();
        try {
            rowCount = ddf.getNumRows();
        } catch (DDFException exception) {
            throw new RuntimeException(exception);
        }
    }

    private String SQL = "CREATE TABLE ? AS SELECT * FROM ? ORDER BY RANDOM() LIMIT ?;";

    //TODO : Change implemtation of CVK and CVRandom

    public List<CrossValidationSet> CVRandom(int k, double trainingSize, long seed) throws DDFException {
        List<CrossValidationSet> finalDDFlist = new ArrayList<CrossValidationSet>();
        long resultSize = rowCount / ((int) trainingSize);
        for (int i = 0; i < k; i++) {
            String tempView = "temp" + date.getTime() + i;
            executeDDL(SQL, tempView + "test", (long)(resultSize*0.3));
            executeDDL(SQL, tempView + "train", (long)(resultSize*0.7));
            DDF trainDDF = create(tempView + "train");
            DDF testDDF = create(tempView + "test");
            finalDDFlist.add(new CrossValidationSet(trainDDF,testDDF));
        }
        return finalDDFlist;
    }

    public DDF create(String table) {
        Schema tableSchema = ddf.getSchema();
        TableNameRepresentation emptyRep = new TableNameRepresentation(table, tableSchema);
        try {
            return awsddfManager.newDDF(awsddfManager, emptyRep, Identifiers.representation(),
                    ddf.getNamespace(), table, tableSchema);
        } catch (DDFException exception) {
            throw new RuntimeException(exception);
        }
    }

    public List<CrossValidationSet> CVK(int k, long seed) {
        List<CrossValidationSet> finalDDFlist = new ArrayList<CrossValidationSet>();
        long resultSize = rowCount / k;
        for (int i = 0; i < k; i++) {
            String tempView = "temp" + date.getTime() + i;
            executeDDL(SQL, tempView + "test", resultSize);
            executeDDL(SQL, tempView + "train", resultSize);
            DDF trainDDF = create(tempView + "train");
            DDF testDDF = create(tempView + "test");
            finalDDFlist.add(new CrossValidationSet(trainDDF,testDDF));
        }
        return finalDDFlist;
    }

    public void executeDDL(String ddlString, String temp, long resultSize) {
        try (Connection conn = awsddfManager.getConnection();) {
            try (PreparedStatement stmt = conn.prepareStatement(ddlString);) {
                stmt.setString(1, temp);
                stmt.setString(2, ddf.getTableName());
                stmt.setLong(3, resultSize);
                stmt.executeUpdate();
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }


}