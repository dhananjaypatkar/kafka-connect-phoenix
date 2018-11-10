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

package io.kafka.connect.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Dhananjay
 *
 */
public class PhoenixClient {
	
	
	private static final Logger log = LoggerFactory.getLogger(PhoenixClient.class);

	/**
	 * 
	 */
	private PhoenixConnectionManager connectionManager;
	
	private static final int COMMIT_INTERVAL  = 100;
	
	/**
	 * @param connectionManager
	 */
	public PhoenixClient(final PhoenixConnectionManager connectionManager){
		this.connectionManager = connectionManager;
	}
	
	
	/**
	 * @param memberId
	 * @param schema
	 * @param tableName
	 * @return
	 */
	private String formUpsert(final Schema schema, final String tableName,final String cf){
		String[] namespace= tableName.split("\\.");
		StringBuilder query = new StringBuilder("upsert into \""+namespace[0] +"\".\""+ namespace[1]+"\"(");
		StringBuilder query_part2 = new StringBuilder(") values (");
		schema.fields().stream().forEach(f -> {query.append("\"" +f.name()+"\","); query_part2.append("?,");} );
		query.deleteCharAt(query.lastIndexOf(","));
		query_part2.deleteCharAt(query_part2.lastIndexOf(","));
		query.append(query_part2).append(")");
		log.debug("Query formed "+query);
		return query.toString();}
	
	
	public void execute(final String tableName,final Schema schema,List<Map<String,Object>> records){
		String cf = tableName.split("\\.")[1];
		try(final Connection connection = this.connectionManager.getConnection();
			final PreparedStatement ps = connection.prepareStatement(formUpsert( schema, tableName,cf))
			){
				connection.setAutoCommit(false);
				final AtomicInteger rowCounter = new AtomicInteger(0);
				records.stream().forEach(r ->{
				int paramIndex = 1;
					try {
					
						ps.setString(paramIndex++, String.valueOf(r.get("ROWKEY")));
						
						//Iterate over fields
						List<Field> fields = schema.fields();
						for(int i=0; i<fields.size(); i++){
							Field f = fields.get(i);
							Object value = r.get(f.name());
							//log.error("field "+f.name() +" Going for value "+String.valueOf(value));
							Schema sch = f.schema();
							switch(sch.type()){
							case STRING:{
								if(value != null){
									ps.setString(paramIndex++,String.valueOf(value));
								}
								else
									ps.setNull(paramIndex++, Types.VARCHAR);
								}
							break;
							case BOOLEAN:{
								if(value != null){
									ps.setBoolean(paramIndex++,Boolean.getBoolean(String.valueOf(value)));
								}else{
									ps.setNull(paramIndex++, Types.BOOLEAN);
								}
							}
							break;
							case BYTES: {
								if(value != null){
									ps.setBytes(paramIndex++, DatatypeConverter.parseBase64Binary((String) value));
								}else{
									ps.setNull(paramIndex++, Types.BINARY);
								}
							}
							break;	
							case FLOAT32:
							case FLOAT64: {
									if(value != null){
										ps.setDouble(paramIndex++, Double.valueOf(String.valueOf(value)));
									}else{
										ps.setNull(paramIndex++, Types.FLOAT);
									}
							}
							break;							
							case INT8:
							case INT16:
							case INT32:
							case INT64:{
									if("org.apache.kafka.connect.data.Timestamp".equals(sch.name())){
										if(value != null){
											ps.setTimestamp(paramIndex++,new Timestamp(Long.valueOf(String.valueOf(value))) );
										}else{
											ps.setNull(paramIndex++, Types.TIMESTAMP);
										}
									}else{
										if(value != null){
											ps.setLong(paramIndex++, Long.valueOf(String.valueOf(value)));
										}else{
											ps.setNull(paramIndex++, Types.BIGINT);
										}
									}
								}
							break;
							}
						}
						ps.executeUpdate();
						
						if(rowCounter.incrementAndGet()%COMMIT_INTERVAL == 0){
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
