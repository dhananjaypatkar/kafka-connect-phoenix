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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
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

  private Map<String, List<String>> oldTableFieldNames;

  private static final int COMMIT_INTERVAL = 100;

  /**
   *
   */
  public PhoenixClient(final PhoenixConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
    this.oldTableFieldNames = new HashedMap();
  }

  /**
   *
   */
  public String formUpsert(final Schema schema, final String tableName) {
    StringBuilder query = new StringBuilder("upsert into ");
    query.append(convertTableName(tableName));
    query.append(" (");
    query.append(schema.fields().stream().map(f -> "\"" + f.name() + "\"")
        .collect(Collectors.joining(",")));
    query.append(") values (");
    query.append(schema.fields().stream().map(f -> "?")
        .collect(Collectors.joining(",")));
    query.append(")");
    log.debug("Query formed " + query);
    return query.toString();
  }

  public String formDelete(final String tableName, String[] rowkeyColumns) {
    StringBuilder query = new StringBuilder("delete from ");
    query.append(convertTableName(tableName));
    query.append(" where ");
    query.append(Arrays.stream(rowkeyColumns).map(c -> "\"" + c + "\" = ?")
        .collect(Collectors.joining(" and ")));
    log.debug("Query formed " + query);
    return query.toString();
  }

  public String formCreate(final String tableName, final Schema schema, String[] rowkeyColumns) {
    StringBuilder query = new StringBuilder("create table if not exists ");
    query.append(
        convertTableName(tableName));
    query.append(" (");
    query.append(schema.fields().stream()
        .map(f -> "\"" + f.name() + "\" " + convertToPhoenixType(f))
        .collect(Collectors.joining(",")));
    query.append(",constraint pk primary key (");
    query.append(Arrays.stream(rowkeyColumns).map(k -> "\"" + k + "\"")
        .collect(Collectors.joining(",")));
    query.append(")");
    query.append(")");
    log.debug("Query formed " + query);
    return query.toString();
  }

  public String formDropColumn(final String tableName, String fieldName) {
    StringBuilder query = new StringBuilder("alter table ");
    query.append(convertTableName(tableName));
    query.append(" drop column ");
    query.append("\"" + fieldName + "\"");
    log.debug("Query formed " + query);
    return query.toString();
  }

  public String formAddColumn(final String tableName, Field field) {
    StringBuilder query = new StringBuilder("alter table ");
    query.append(convertTableName(tableName));
    query.append(" add ");
    query.append("\"" + field.name() + "\"");
    query.append(" ");
    query.append(convertToPhoenixType(field));
    Schema schema = field.schema();
    Object defaultValue = schema.defaultValue();
    if (defaultValue != null) {
      query.append(" ");
      query.append("default ");
      if (Type.STRING.equals(schema.type())) {
        query.append("'");
        query.append(defaultValue);
        query.append("'");
      } else {
        query.append(defaultValue);
      }
    }
    log.debug("Query formed " + query);
    return query.toString();
  }

  private String convertTableName(String tableName) {
    return Arrays.stream(tableName.split("\\.")).map(n -> "\"" + n + "\"")
        .collect(Collectors.joining("."));
  }

  private String convertToPhoenixType(Field f) {
    String fType;
    Schema fSchema = f.schema();
    switch (fSchema.type()) {
      case STRING: {
        fType = "varchar";
        break;
      }
      case BOOLEAN: {
        fType = "boolean";
        break;
      }
      case BYTES: {
        fType = "varbinary";
        break;
      }
      case FLOAT32:
      case FLOAT64: {
        fType = "double";
        break;
      }
      case INT8: {
        fType = "tinyint";
        break;
      }
      case INT16: {
        fType = "smallint";
        break;
      }
      case INT32: {
        fType = "integer";
        break;
      }
      case INT64: {
        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(fSchema.name())) {
          fType = "timestamp";
        } else {
          fType = "bigint";
        }
        break;
      }
      default:
        throw new RuntimeException("unknown type");
    }
    return fType;
  }

  private int bindValue(PreparedStatement preparedStatement, int paramIndex, Object value, Field f)
      throws SQLException {
    Schema sch = f.schema();
    switch (sch.type()) {
      case STRING: {
        if (value != null) {
          preparedStatement.setString(paramIndex++, String.valueOf(value));
        } else {
          preparedStatement.setNull(paramIndex++, Types.VARCHAR);
        }
      }
      break;
      case BOOLEAN: {
        if (value != null) {
          preparedStatement.setBoolean(paramIndex++, Boolean.getBoolean(String.valueOf(value)));
        } else {
          preparedStatement.setNull(paramIndex++, Types.BOOLEAN);
        }
      }
      break;
      case BYTES: {
        if (value != null) {
          preparedStatement.setBytes(paramIndex++,
              DatatypeConverter.parseBase64Binary((String) value));
        } else {
          preparedStatement.setNull(paramIndex++, Types.BINARY);
        }
      }
      break;
      case FLOAT32:
      case FLOAT64: {
        if (value != null) {
          preparedStatement.setDouble(paramIndex++, Double.valueOf(String.valueOf(value)));
        } else {
          preparedStatement.setNull(paramIndex++, Types.FLOAT);
        }
      }
      break;
      case INT8:
      case INT16:
      case INT32:
      case INT64: {
        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(sch.name())) {
          if (value != null) {
            preparedStatement.setTimestamp(paramIndex++,
                new Timestamp(Long.valueOf(String.valueOf(value))));
          } else {
            preparedStatement.setNull(paramIndex++, Types.TIMESTAMP);
          }
        } else {
          if (value != null) {
            preparedStatement.setLong(paramIndex++, Long.valueOf(String.valueOf(value)));
          } else {
            preparedStatement.setNull(paramIndex++, Types.BIGINT);
          }
        }
      }
      break;
      default:
        throw new RuntimeException("unknown type");
    }
    return paramIndex;
  }

  private void updateTableFieldNames(String tableName, String schemaFile, List<String> oldNames) {
    oldTableFieldNames.put(tableName, oldNames);
    try {
      Files.write(Paths.get(schemaFile),
          oldNames.stream().collect(Collectors.joining(",")).getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void execute(final String tableName, final Schema schema, String[] rowkeyColumns,
      List<Map<String, Object>> records, final String schemaFile) {
    final Schema rowSchema = schema.field(DebeziumConstants.FIELD_NAME_BEFORE).schema();
    updateSchema(tableName, rowkeyColumns, schemaFile, rowSchema);
    try (final Connection connection = this.connectionManager.getConnection();
        final PreparedStatement upsertPreSt = connection
            .prepareStatement(formUpsert(rowSchema, tableName));
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
            Map<String, Object> r1 =
                (Map<String, Object>) r.get(DebeziumConstants.FIELD_NAME_BEFORE);
            for (int i = 0; i < rowkeyColumns.length; i++) {
              String rowkeyColumn = rowkeyColumns[i];
              Object value = r1.get(rowkeyColumn);
              Field f = rowSchema.field(rowkeyColumn);
              paramIndex = bindValue(deletePreSt, paramIndex, value, f);
            }
            deletePreSt.executeUpdate();
          } else {
            int paramIndex = 1;
            Map<String, Object> r1 = (Map<String, Object>) r
                .get(DebeziumConstants.FIELD_NAME_AFTER);
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

  private void updateSchema(String tableName, String[] rowkeyColumns, String schemaFile,
      Schema rowSchema) {
    final List<String> oldNames = new ArrayList<>();
    if (oldTableFieldNames.size() == 0 || !oldTableFieldNames.containsKey(tableName)) {
      try {
        String oldFiledNames = new String(Files.readAllBytes(Paths.get(schemaFile)));
        if (oldFiledNames != null && oldFiledNames.length() > 0) {
          oldNames.addAll(Arrays.asList(oldFiledNames.split(",")));
          oldTableFieldNames.put(tableName, oldNames);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      oldNames.addAll(oldTableFieldNames.get(tableName));
    }
    if (oldNames.size() == 0) {
      try (final Connection connection = this.connectionManager.getConnection();
          final Statement schemaPreSt = connection.createStatement()) {
        connection.setAutoCommit(false);
        schemaPreSt.execute(formCreate(tableName, rowSchema, rowkeyColumns));
        updateTableFieldNames(tableName, schemaFile,
            rowSchema.fields().stream().map(Field::name).collect(Collectors.toList()));
        connection.commit();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else {
      List<String> newNames = rowSchema.fields().stream().map(Field::name)
          .collect(Collectors.toList());
      log.debug("old names: {}", oldNames);
      log.debug("new names: {}", newNames);
      List<String> dropNames = oldNames.stream().filter(n -> !newNames.contains(n))
          .collect(Collectors.toList());
      List<Field> addFields = rowSchema.fields().stream()
          .filter(f -> !oldNames.contains(f.name()))
          .collect(Collectors.toList());
      if (dropNames.size() > 0 || addFields.size() > 0) {
        try (final Connection connection = this.connectionManager.getConnection();
            final Statement schemaSt = connection.createStatement()) {
          connection.setAutoCommit(false);
          if (dropNames.size() > 0) {
            log.debug("drop names: {}", dropNames);
            for (int i = 0; i < dropNames.size(); i++) {
              schemaSt.execute(formDropColumn(tableName, dropNames.get(i)));
            }
          }
          if (addFields.size() > 0) {
            log.debug("add fields: {}", addFields);
            for (int i = 0; i < addFields.size(); i++) {
              schemaSt.execute(formAddColumn(tableName, addFields.get(i)));
            }
          }
          updateTableFieldNames(tableName, schemaFile, newNames);
          connection.commit();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
