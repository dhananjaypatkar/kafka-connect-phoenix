package io.kafka.connect.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 
 * @author Dhananjay
 *
 */
public class PhoenixConnectionManager {

	private String phoenixURL;
	
	public PhoenixConnectionManager(final String phoenixurl) {
		this.phoenixURL = phoenixurl;
	}
	
	public void init() throws ClassNotFoundException {
		Class.forName("org.apache.phoenix.queryserver.client.Driver");
	}
	
	/**
	 * https://phoenix.apache.org/faq.html#Should_I_pool_Phoenix_JDBC_Connections
	 * 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Connection conn =  DriverManager.getConnection(String.format("jdbc:phoenix:thin:url=%s;serialization=PROTOBUF",phoenixURL));
		return conn;
	}
	
}
