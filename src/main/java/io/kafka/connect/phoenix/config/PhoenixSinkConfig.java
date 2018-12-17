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

package io.kafka.connect.phoenix.config;

import com.google.common.base.Preconditions;
import io.kafka.connect.phoenix.parser.EventParser;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;

/**
 *
 * @author Dhananjay
 *
 */
public class PhoenixSinkConfig extends AbstractConfig {

  public static final String PQS_URL = "pqs.url";
  public static final String EVENT_PARSER_CONFIG = "event.parser.class";
  public static final String TABLE_ROWKEY_COLUMNS_TEMPLATE = "hbase.%s.rowkey.columns";
  public static final String HBASE_TABLE_NAME = "hbase.%s.table.name";

  public static ConfigDef CONFIG = new ConfigDef();
  private Map<String, String> properties;

  static {

    CONFIG.define(PQS_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
        "Phoenix Query Server url http://host:8765 " +
            "of the hbase cluster");

    CONFIG.define(EVENT_PARSER_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
        "Event parser class " +
            "to parse the SinkRecord");
  }

  public PhoenixSinkConfig(Map<String, String> originals) {
    this(CONFIG, originals);
  }

  public PhoenixSinkConfig(ConfigDef definition, Map<String, String> originals) {
    super(definition, originals);
    this.properties = originals;
  }

  /**
   * Validates the properties to ensure the rowkey property is configured for each table.
   */
  public void validate() {
    final String topicsAsStr = properties.get(SinkConnectorConfig.TOPICS_CONFIG);
        /*  final String[] topics = topicsAsStr.split(",");
       for(String topic : topics) {
            String key = String.format(TABLE_ROWKEY_COLUMNS_TEMPLATE, topic);
            if(!properties.containsKey(key)) {
                throw new ConfigException(String.format(" No rowkey has been configured for table [%s]", key));
            }
        }*/
  }

  /**
   * Instantiates and return the event parser .
   * @return
   */
  @SuppressWarnings(value = "unchecked")
  public EventParser eventParser() {
    try {
      final String eventParserClass = getString(EVENT_PARSER_CONFIG);
      final Class<? extends EventParser> eventParserImpl = (Class<? extends EventParser>) Class
          .forName(eventParserClass);
      return eventParserImpl.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param propertyName
   * @param defaultValue
   * @return
   */
  public String getPropertyValue(final String propertyName, final String defaultValue) {
    String propertyValue = getPropertyValue(propertyName);
    return propertyValue != null ? propertyValue : defaultValue;
  }

  /**
   * @param propertyName
   * @return
   */
  public String getPropertyValue(final String propertyName) {
    Preconditions.checkNotNull(propertyName);
    return this.properties.get(propertyName);
  }
}
