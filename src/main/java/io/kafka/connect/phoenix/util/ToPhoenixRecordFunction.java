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

package io.kafka.connect.phoenix.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.kafka.connect.phoenix.config.PhoenixSinkConfig;
import io.kafka.connect.phoenix.parser.EventParser;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Dhananjay
 *
 */
public class ToPhoenixRecordFunction implements Function<SinkRecord, Map<String, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ToPhoenixRecordFunction.class);

  private final PhoenixSinkConfig sinkConfig;

  private final EventParser eventParser;

  public ToPhoenixRecordFunction(final PhoenixSinkConfig sinkConfig) {
    this.sinkConfig = sinkConfig;
    this.eventParser = this.sinkConfig.eventParser();
  }

  @Override
  public Map<String, Object> apply(SinkRecord sinkRecord) {
    try {
      Preconditions.checkNotNull(sinkRecord);
      final Map<String, Object> valuesMap = this.eventParser.parseValueObject(sinkRecord);
      return valuesMap;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(
          "Exception while parsing sink record from topic " + sinkRecord.topic() + " key "
              + sinkRecord.key(),
          e);
      //TODO send message to error topic...
      throw new RuntimeException(e);
    }
  }


  /**
   * A kafka topic is a 1:1 mapping to a HBase table.
   * @param table
   * @return
   */
  public String[] rowkeyColumns(final String table) {
    final String entry = String.format(PhoenixSinkConfig.TABLE_ROWKEY_COLUMNS_TEMPLATE, table);
    final String entryValue = sinkConfig.getPropertyValue(entry);
    return entryValue.split(",");
  }

  /**
   * Returns the name space based table for given topic name.
   * This derives name space based on the member partition of the sink record received.
   *
   *
   */
  public String tableName(final String topic) {
    return sinkConfig.getPropertyValue(String.format(PhoenixSinkConfig.HBASE_TABLE_NAME, topic));
  }
}
