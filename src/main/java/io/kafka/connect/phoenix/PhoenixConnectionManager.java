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
