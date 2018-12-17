/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.kafka.connect.phoenix;

import io.kafka.connect.phoenix.util.DebeziumConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dhananjay
 */
public class PhoenixClient {


  private static final Logger log = LoggerFactory.getLogger(PhoenixClient.class);

  /**
   *
   */
  private PhoenixConnectionManager connectionManager;

  private static final int COMMIT_INTERVAL = 100;

  /**
   *
   */
  public PhoenixClient(final PhoenixConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }


  /**
   *
   */
  public String formUpsert(final Schema schema, final String tableName) {
    StringBuilder query = new StringBuilder("upsert into ");
    query.append(String.join(".", Arrays.stream(tableName.split("\\.")).map(n -> "\"" + n + "\"")
        .collect(Collectors.toList())));
    query.append(" (");
    query.append(String.join(",", schema.fields().stream().map(f -> "\"" + f.name() + "\"")
        .collect(Collectors.toList())));
    query.append(") values (");
    query.append(String.join(",", schema.fields().stream().map(f -> "?")
        .collect(Collectors.toList())));
    query.append(")");
    log.debug("Query formed " + query);
    return query.toString();
  }

  public String formDelete(final String tableName, String[] rowkeyColumns) {
    StringBuilder query = new StringBuilder("delete from ");
    query.append(String.join(".", Arrays.stream(tableName.split("\\.")).map(n -> "\"" + n + "\"")
        .collect(Collectors.toList())));
    query.append(" where ");
    query.append(String.join(" and ", Arrays.stream(rowkeyColumns).map(c -> "\"" + c + "\" = ?")
        .collect(Collectors.toList())));
    log.debug("Query formed " + query);
    return query.toString();
  }

  private int bindValue(PreparedStatement ps1, int paramIndex, Object value, Field f)
      throws SQLException {
    Schema sch = f.schema();
    switch (sch.type()) {
      case STRING: {
        if (value != null) {
          ps1.setString(paramIndex++, String.valueOf(value));
        } else {
          ps1.setNull(paramIndex++, Types.VARCHAR);
        }
      }
      break;
      case BOOLEAN: {
        if (value != null) {
          ps1.setBoolean(paramIndex++, Boolean.getBoolean(String.valueOf(value)));
        } else {
          ps1.setNull(paramIndex++, Types.BOOLEAN);
        }
      }
      break;
      case BYTES: {
        if (value != null) {
          ps1.setBytes(paramIndex++,
              DatatypeConverter.parseBase64Binary((String) value));
        } else {
          ps1.setNull(paramIndex++, Types.BINARY);
        }
      }
      break;
      case FLOAT32:
      case FLOAT64: {
        if (value != null) {
          ps1.setDouble(paramIndex++, Double.valueOf(String.valueOf(value)));
        } else {
          ps1.setNull(paramIndex++, Types.FLOAT);
        }
      }
      break;
      case INT8:
      case INT16:
      case INT32:
      case INT64: {
        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(sch.name())) {
          if (value != null) {
            ps1.setTimestamp(paramIndex++,
                new Timestamp(Long.valueOf(String.valueOf(value))));
          } else {
            ps1.setNull(paramIndex++, Types.TIMESTAMP);
          }
        } else {
          if (value != null) {
            ps1.setLong(paramIndex++, Long.valueOf(String.valueOf(value)));
          } else {
            ps1.setNull(paramIndex++, Types.BIGINT);
          }
        }
      }
      break;
      default:
        throw new RuntimeException("unknown type");
    }
    return paramIndex;
  }

  public void execute(final String tableName, final Schema schema, String[] rowkeyColumns,
      List<Map<String, Object>> records) {
    final Schema rowSchema = schema.field(DebeziumConstants.FIELD_NAME_BEFORE).schema();
    try (final Connection connection = this.connectionManager.getConnection();
        final PreparedStatement upsertPreSt = connection.prepareStatement(formUpsert(rowSchema, tableName));
        final PreparedStatement deletePreSt = connection
            .prepareStatement(formDelete(tableName, rowkeyColumns))
    ) {
      connection.setAutoCommit(false);
      final AtomicInteger rowCounter = new AtomicInteger(0);
      records.stream().forEach(r -> {
        try {
          String op = String.valueOf(r.get(DebeziumConstants.FIELD_NAME_OP));
          if (DebeziumConstants.OPERATION_DELETE.equals(op)) {
            int paramIndex = 1;
            Map<String, Object> r1 = (Map<String, Object>) r.get(DebeziumConstants.FIELD_NAME_BEFORE);
            for (int i = 0; i < rowkeyColumns.length; i++) {
              String rowkeyColumn = rowkeyColumns[i];
              Object value = r1.get(rowkeyColumn);
              Field f = rowSchema.field(rowkeyColumn);
              paramIndex = bindValue(deletePreSt, paramIndex, value, f);
            }
            deletePreSt.executeUpdate();
          } else {
            int paramIndex = 1;
            Map<String, Object> r1 = (Map<String, Object>) r.get(DebeziumConstants.FIELD_NAME_AFTER);
            List<Field> fields = rowSchema.fields();
            for (int i = 0; i < fields.size(); i++) {
              Field f = fields.get(i);
              Object value = r1.get(f.name());
              paramIndex = bindValue(upsertPreSt, paramIndex, value, f);
            }
            upsertPreSt.executeUpdate();
          }
          if (rowCounter.incrementAndGet() % COMMIT_INTERVAL == 0) {
            connection.commit();
            rowCounter.set(0);
          }

        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
