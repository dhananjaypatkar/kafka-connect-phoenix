package io.kafka.connect.phoenix.util;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import io.kafka.connect.phoenix.config.PhoenixSinkConfig;
import io.kafka.connect.phoenix.parser.EventParser;

/**
 * 
 * @author Dhananjay
 *
 */
public class ToPhoenixRecordFunction implements Function<SinkRecord, Map<String, Object>>  {

	private static final Logger LOGGER = LoggerFactory.getLogger(ToPhoenixRecordFunction.class);
    
	private final PhoenixSinkConfig sinkConfig;
    
    private final EventParser eventParser;
    
    public ToPhoenixRecordFunction(final PhoenixSinkConfig sinkConfig){
    	this.sinkConfig = sinkConfig;
    	this.eventParser = this.sinkConfig.eventParser();
    }
	
	@Override
	public Map<String,Object> apply(SinkRecord sinkRecord) {
		try {
			Preconditions.checkNotNull(sinkRecord);
			final String table = sinkRecord.topic();
			final String delimiter = rowkeyDelimiter(sinkRecord.topic());
			final Map<String, Object> valuesMap = this.eventParser.parseValueObject(sinkRecord);
			final Map<String, Object> keysMap = this.eventParser.parseKeyObject(sinkRecord);

			valuesMap.putAll(keysMap);
			final String[] rowkeyColumns = rowkeyColumns(table);
			final String rowkey = toRowKey(valuesMap, rowkeyColumns, delimiter);

			valuesMap.put("ROWKEY", rowkey);
			
			return valuesMap;
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error(
					"Exception while parsing sink record from topic " + sinkRecord.topic() + " key " + sinkRecord.key(),
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
    private String[] rowkeyColumns(final String table) {
        final String entry = String.format(PhoenixSinkConfig.TABLE_ROWKEY_COLUMNS_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry);
        return entryValue.split(",");
    }

    /**
     * Returns the delimiter for a table. If nothing is configured in properties,
     * we use the default {@link PhoenixSinkConfig#DEFAULT_HBASE_ROWKEY_DELIMITER}
     * @param table hbase table.
     * @return
     */
    private String rowkeyDelimiter(final String table) {
        final String entry = String.format(PhoenixSinkConfig.TABLE_ROWKEY_DELIMITER_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry, PhoenixSinkConfig.DEFAULT_HBASE_ROWKEY_DELIMITER);
        return entryValue;
    }
	
    /**
     * Returns the column family mapped in configuration for the table.  If not present, we use the
     * default {@link PhoenixSinkConfig#DEFAULT_HBASE_COLUMN_FAMILY}
     * @param table hbase table.
     * @return
     */
    private String columnFamily(final String table) {
        final String entry = String.format(PhoenixSinkConfig.TABLE_COLUMN_FAMILY_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry, PhoenixSinkConfig.DEFAULT_HBASE_COLUMN_FAMILY);
        return entryValue;
    }

    /**
     * Returns the name space based table for given topic name.
     * This derives name space based on the member partition of the sink record received.
     * 
     *
     */
    public String tableName(final String topic) {
        return String.format(PhoenixSinkConfig.HBASE_TABLE_NAME, topic);
    }
    
    
    /**
    *
    * @param valuesMap
    * @param columns
    * @return
    */
   private String toRowKey(final Map<String, Object> valuesMap, final String[] columns, final String delimiter) {
       Preconditions.checkNotNull(valuesMap);
       Preconditions.checkNotNull(delimiter);
       String rowkey = null;
      // byte[] delimiterBytes = Bytes.toBytes(delimiter);
       //For phoenix we need single zero/ null byte byte as de-limiter
       Set<String> keys = valuesMap.keySet();
       for(String column : columns) {
       	for(String key : keys){
       		if(key.equalsIgnoreCase(column)){
       			Object columnValue = valuesMap.get(key);
                   if(rowkey == null) {
                       rowkey = String.valueOf(columnValue);
                   } else {
                       rowkey = rowkey + "|"+ String.valueOf(columnValue);
                   }
                   break;
       		}
           }
       }
       return rowkey;
   }

	public EventParser getEventParser() {
		return eventParser;
	}
}
