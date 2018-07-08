package io.kafka.connect.phoenix.sink;

import com.google.common.collect.Lists;

import io.kafka.connect.phoenix.config.PhoenixSinkConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author Dhananjay
 *
 */
public class PhoenixSinkConnector extends SinkConnector {

    public static final String VERSION = "1.0";
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PhoenixSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = Lists.newArrayList();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProperties);
        }
        return configs;
    }

    @Override
    public void stop() {
        // NO-OP
    }

	@Override
	public ConfigDef config() {
		return PhoenixSinkConfig.CONFIG;
	}
}