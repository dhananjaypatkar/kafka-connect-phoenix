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

package io.kafka.connect.phoenix.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 *
 * @author Dhananjay
 *
 */
public class PhoenixSchemaInfo {

  private Schema schema;

  private String tableName;


  public PhoenixSchemaInfo(final String tableName, final Schema schema) {
    this.tableName = tableName;
    this.schema = schema;
  }


  public static PhoenixSchemaInfo build(SinkRecord sr) {
    return new PhoenixSchemaInfo(sr.topic(), sr.valueSchema());
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PhoenixSchemaInfo other = (PhoenixSchemaInfo) obj;
		/*if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;*/
    if (tableName == null) {
      if (other.tableName != null) {
        return false;
      }
    } else if (!tableName.equals(other.tableName)) {
      return false;
    }
    return true;
  }


  public Schema getSchema() {
    return schema;
  }

  public String getTableName() {
    return tableName;
  }


  @Override
  public String toString() {
    return "PhoenixSchemaInfo [schema=" + schema + ", tableName=" + tableName + "]";
  }

}