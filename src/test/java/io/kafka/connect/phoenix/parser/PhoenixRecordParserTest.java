/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kafka.connect.phoenix.parser;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kafka.connect.phoenix.parser.PhoenixRecordParser;

/**
 * @author Dhananjay
 * 
 */
public class PhoenixRecordParserTest {

    private PhoenixRecordParser eventParser;

    @Before
    public void setup() {
        eventParser = new PhoenixRecordParser(); 
    }

    @Test
    public void testParseValue() {
        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
          .field("url", Schema.STRING_SCHEMA)
          .field("id", Schema.INT64_SCHEMA)
          .field("zipcode", Schema.INT32_SCHEMA)
          .field("status", Schema.BOOLEAN_SCHEMA)
          .field("data", Schema.BYTES_SCHEMA)
          .field("salary", Schema.FLOAT32_SCHEMA)
          .build();
        
        valueSchema.fields().stream().forEach(f ->{System.out.println(f.schema().name());});

        String url = "google.com";
        long id = -1;
        int zipcode = 95051;
        boolean status = true;
        String data  = "Dhananjay";
        float salary = 90.299F;
        
        final Struct record = new Struct(valueSchema)
          .put("url", url)
          .put("id", id)
          .put("zipcode", zipcode).put("salary", salary)//.put("unknown", "kkk")
          .put("status", status).put("data",data.getBytes());

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, valueSchema, record, 0);

        Map<String, Object> result = eventParser.parseValueObject(sinkRecord);
        Assert.assertEquals(6, result.size());
        
      
    }

    @Test
    public void testParseNullKey() {
        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, null, 0);
        final Map<String, Object> keys = eventParser.parseKeyObject(sinkRecord);
        Assert.assertTrue(keys.isEmpty());
    }
    
   @Test
    public void testParseNullValue(){
	   final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, null, 0);
	   final Map<String, Object> values = eventParser.parseValueObject(sinkRecord);
	   Assert.assertTrue(values.isEmpty());
   }

	
}
