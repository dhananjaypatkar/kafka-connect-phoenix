package io.kafka.connect.phoenix.parser;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author Dhananjay
 *
 */
public class PhoenixRecordParser implements EventParser {
	
	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final JsonConverter keyConverter;

	private final JsonConverter valueConverter;
	
	/**
	 * default c.tor
	 */
	public PhoenixRecordParser() {
		this.keyConverter = new JsonConverter();
		this.valueConverter = new JsonConverter();

		Map<String, String> props = new HashMap<>(1);
		props.put("schemas.enable", Boolean.TRUE.toString());

		this.keyConverter.configure(props, true);
		this.valueConverter.configure(props, false);

	}
	
	
	public Map<String, Object> parse(final String topic, final Schema schema, final Object value, final boolean isKey)
			throws EventParsingException {
		try {
			byte[] valueBytes = null;
			if (isKey) {
				valueBytes = keyConverter.fromConnectData(topic, schema, value);
			} else {
				valueBytes = valueConverter.fromConnectData(topic, schema, value);
			}
			if (valueBytes == null || valueBytes.length == 0) {
				return Collections.emptyMap();
			}

			Map<String, Object> keyValues = new HashMap<>();
			final JsonNode valueNode = OBJECT_MAPPER.readTree(new ByteArrayInputStream(valueBytes));
			keyValues = OBJECT_MAPPER.convertValue(valueNode.get("payload"), new TypeReference<Map<String, Object>>() {
			});
			
			return keyValues;
		} catch (Exception ex) {
			final String errorMsg = String.format("Failed to parse the schema [%s] , value [%s] with ex [%s]", schema,
					value, ex.getMessage());
			throw new EventParsingException(errorMsg, ex);
		}
	}
	
	
	@Override
	public Map<String, Object> parseKeyObject(SinkRecord sr) throws EventParsingException {
		Map<String,Object> map =this.parse(sr.topic(), sr.keySchema(), sr.key(), true);
		return map;
	}

	@Override
	public Map<String, Object> parseValueObject(SinkRecord sr) throws EventParsingException {
		Map<String,Object> map  = this.parse(sr.topic(), sr.valueSchema(), sr.value(), false);
		return map;
	}

}
