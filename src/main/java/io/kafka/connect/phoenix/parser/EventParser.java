package io.kafka.connect.phoenix.parser;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * 
 * @author Dhananjay
 *
 */
public interface EventParser {   
    /**
     * Parses the key value based on the key schema .
     * @param sr
     * @return
     */
    Map<String, Object> parseKeyObject(SinkRecord sr) throws EventParsingException;

    /**
     * Parses the values based on the value schema.
     * @param sr
     * @return
     */
    Map<String, Object> parseValueObject(SinkRecord sr) throws EventParsingException;
}
