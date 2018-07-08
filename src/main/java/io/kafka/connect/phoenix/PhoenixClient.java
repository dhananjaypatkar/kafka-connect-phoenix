package io.kafka.connect.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;

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
		StringBuilder query = new StringBuilder("upsert into \""+namespace[0] +"\".\""+ namespace[1]+"\"(ROWKEY");
		StringBuilder query_part2 = new StringBuilder(") values (?");
		schema.fields().stream().forEach(f -> {query.append(","+"\""+cf+"info"+"\"."+"\"" +f.name()+"\""); query_part2.append(",?");} );
		query.append(query_part2).append(")");
		log.error("Query formed "+query);
		return query.toString();
	}
	
	
	public void execute(final String tableName,final Schema schema,List<Map<String,Object>> records){
		String cf = tableName.split("\\.")[1];
		try(final Connection connection = this.connectionManager.getConnection();
			final PreparedStatement ps = connection.prepareStatement(formUpsert( schema, tableName,cf))
			){
				connection.setAutoCommit(false);
				records.stream().forEach(r ->{
				int paramIndex = 1;
					try {
					
						ps.setString(paramIndex++, String.valueOf(r.get("ROWKEY")));
						
						//Iterate over fields
						List<Field> fields = schema.fields();
						for(int i=0; i<fields.size(); i++){
							Field f = fields.get(i);
							log.error("Setting field name "+f.name() + "Type "+f.schema().type().name());
							Object value = r.get(f.name());
							//log.error("field "+f.name() +" Going for value "+String.valueOf(value));
							Schema sch = f.schema();
							switch(sch.type()){
							case STRING:{
								if(value != null){
									log.error("Setting STRING for "+sch.name() + " value "+value);
									ps.setString(paramIndex++,String.valueOf(value));
								}
								else
									ps.setNull(paramIndex++, Types.VARCHAR);
								}
							break;
							case BOOLEAN:{
								if(value != null){
									log.error("Setting BOOLEAN for "+sch.name() + " value "+value);
									ps.setBoolean(paramIndex++,Boolean.getBoolean(String.valueOf(value)));
								}else{
									ps.setNull(paramIndex++, Types.BOOLEAN);
								}
							}
							break;
							case BYTES: {
								if(value != null){
									log.error("Setting BYTES for "+sch.name() + " value "+value);
									ps.setBytes(paramIndex++, DatatypeConverter.parseBase64Binary((String) value));
								}else{
									ps.setNull(paramIndex++, Types.BINARY);
								}
							}
							break;	
							case FLOAT32:
							case FLOAT64: {
									if(value != null){
										log.error("Setting FLOAT for "+sch.name() + " value "+value);
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
											log.error("Setting int times for "+sch.name() + " value "+value);
											ps.setTimestamp(paramIndex++,new Timestamp(Long.valueOf(String.valueOf(value))) );
										}else{
											ps.setNull(paramIndex++, Types.TIMESTAMP);
										}
									}else{
										if(value != null){
											log.error("Setting INT for "+sch.name() + " value "+value);
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
