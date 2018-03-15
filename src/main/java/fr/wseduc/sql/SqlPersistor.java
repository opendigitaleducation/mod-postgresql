/*
 * Copyright © WebServices pour l'Éducation, 2014
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.wseduc.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.vertx.java.busmods.BusModBase;

import java.sql.*;
import java.text.SimpleDateFormat;

import static fr.wseduc.sql.TimestampEncoderDecoder.encode;

public class SqlPersistor extends BusModBase implements Handler<Message<JsonObject>> {

	private HikariDataSource ds;

	@Override
	public void start() {
		super.start();
		String url = config.getString("url", "jdbc:postgresql://localhost:5432/test");

		HikariConfig conf = new HikariConfig();
		conf.setJdbcUrl(url);
		conf.setUsername(config.getString("username", "postgres"));
		conf.setPassword(config.getString("password", ""));
		conf.setMaximumPoolSize(config.getInteger("pool_size", 10));
		conf.addDataSourceProperty("cachePrepStmts", "true");
		conf.addDataSourceProperty("prepStmtCacheSize", "250");
		conf.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		conf.addDataSourceProperty("useServerPrepStmts", "true");
		ds = new HikariDataSource(conf);

		vertx.eventBus().consumer(config.getString("address", "sql.persistor"), this);
	}

	@Override
	public void stop() throws Exception {
		super.stop();
		if (ds != null) {
			ds.close();
		}
	}

	@Override
	public void handle(Message<JsonObject> message) {
		String action = message.body().getString("action", "");
		switch (action) {
			case "select" :
				doSelect(message);
				break;
			case "insert" :
				doInsert(message);
				break;
			case "prepared" :
				doPrepared(message);
				break;
			case "transaction" :
				doTransaction(message);
				break;
			case "raw" :
				doRaw(message);
				break;
			default :
				sendError(message, "invalid.action");
		}
	}

	private void doRaw(Message<JsonObject> message) {
		Connection connection = null;
		try {
			connection = ds.getConnection();

			JsonObject result = raw(message.body(), connection);
			if (result != null) {
				sendOK(message, result);
			} else {
				sendError(message, "invalid.query");
			}
		} catch (SQLException e) {
			sendError(message, e.getMessage(), e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}


	private JsonObject raw(JsonObject json, Connection connection) throws SQLException {
		String query = json.getString("command");
		if (query == null || query.isEmpty()) {
			return null;
		}
		return raw(query, connection);
	}

	private JsonObject raw(String query, Connection connection) throws SQLException {
		Statement statement = null;
		ResultSet resultSet = null;
		if (logger.isDebugEnabled()) {
			logger.debug("query : " + query);
		}
		try {
			statement = connection.createStatement();
			JsonObject r;
			if (statement.execute(query)) {
				resultSet = statement.getResultSet();
				r = buildResults(resultSet);
			} else {
				r = buildResults(statement.getUpdateCount());
			}
			if (logger.isDebugEnabled()) {
				logger.debug(r.encodePrettily());
			}
			return r;
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
		}
	}

	private JsonObject raw(String query) throws SQLException {
		Connection connection = null;
		try {
			connection = ds.getConnection();
			return raw(query, connection);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	private void doTransaction(Message<JsonObject> message) {
		JsonArray statements = message.body().getJsonArray("statements");
		if (statements == null || statements.size() == 0) {
			sendError(message, "missing.statements");
			return;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("TRANSACTION-JSON: " + statements.encodePrettily());
		}
		Connection connection = null;
		try {
			connection = ds.getConnection();
			connection.setAutoCommit(false);
			JsonArray results = new JsonArray();
			for (Object s : statements) {
				if (!(s instanceof JsonObject)) continue;
				JsonObject json = (JsonObject) s;
				String action = json.getString("action", "");
				switch (action) {
					case "insert":
						results.add(raw(insertQuery(json), connection));
						break;
					case "select":
						results.add(raw(selectQuery(json), connection));
						break;
					case "raw":
						results.add(raw(json, connection));
						break;
					case "prepared":
						results.add(prepared(json, connection));
						break;
					default:
						connection.rollback();
						throw new IllegalArgumentException("invalid.action");
				}
			}
			connection.commit();
			sendOK(message, new JsonObject().put("results", results));
		} catch (Exception e) {
			sendError(message, e.getMessage(), e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	private void doPrepared(Message<JsonObject> message) {
		Connection connection = null;
		try {
			connection = ds.getConnection();

			JsonObject result = prepared(message.body(), connection);
			if (result != null) {
				sendOK(message, result);
			} else {
				sendError(message, "invalid.query");
			}
		} catch (SQLException e) {
			sendError(message, e.getMessage(), e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	private JsonObject prepared(JsonObject json, Connection connection) throws SQLException {
		String query = json.getString("statement");
		JsonArray values = json.getJsonArray("values");
		if (query == null || query.isEmpty() || values == null) {
			return null;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("query : " + query + " - values : " + values.encode());
		}
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(query);
			for (int i = 0; i < values.size(); i++) {
				Object v = values.getValue(i);
				if (v instanceof Integer) {
					statement.setInt(i + 1, (Integer) v);
				} else {
					if (v != null) {
						v = v.toString();
					}
					statement.setObject(i + 1, v);
				}

			}
			JsonObject r;
			if (statement.execute()) {
				resultSet = statement.getResultSet();
				r = buildResults(resultSet);
			} else {
				r = buildResults(statement.getUpdateCount());
			}
			if (logger.isDebugEnabled()) {
				logger.debug(r.encodePrettily());
			}
			return r;
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
		}
	}

	private void doInsert(Message<JsonObject> message) {
		JsonObject json = message.body();
		String query = insertQuery(json);
		if (query == null) {
			sendError(message, "invalid.query");
			return;
		}
		try {
			sendOK(message, raw(query));
		} catch (SQLException e) {
			sendError(message, e.getMessage(), e);
		}
	}

	private String insertQuery(JsonObject json) {
		String table = json.getString("table");
		JsonArray fields = json.getJsonArray("fields");
		JsonArray values = json.getJsonArray("values");
		String returning = json.getString("returning");
		if (table == null || table.isEmpty() || fields == null ||
				fields.size() == 0 || values == null || values.size() == 0) {
			return null;
		}
		StringBuilder sb = new StringBuilder("INSERT INTO ")
				.append(table)
				.append(" (");
		for (Object o : fields) {
			if (!(o instanceof String)) continue;
			sb.append(escapeField((String) o)).append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		sb.append(") VALUES ");
		for (Object row : values) {
			if (row instanceof JsonArray) {
				sb.append("(");
				for (Object o : (JsonArray) row) {
					sb.append(escapeValue(o)).append(",");
				}
				sb.deleteCharAt(sb.length()-1);
				sb.append("),");
			}
		}
		sb.deleteCharAt(sb.length()-1);
		if (returning != null) {
			sb.append(" RETURNING ").append(returning);
		}
		return sb.toString();
	}

	private void doSelect(Message<JsonObject> message) {
		JsonObject json = message.body();
		String query = selectQuery(json);
		if (query == null) {
			sendError(message, "invalid.query");
			return;
		}
		try {
			sendOK(message, raw(query));
		} catch (SQLException e) {
			sendError(message, e.getMessage(), e);
		}
	}

	private String selectQuery(JsonObject json) {
		String table = json.getString("table");
		JsonArray fields = json.getJsonArray("fields");
		if (table == null || table.isEmpty()) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		if (fields != null && fields.size() > 0) {
			sb.append("SELECT ");
			for (Object o : fields) {
				if (!(o instanceof String)) continue;
				sb.append(escapeField(o.toString())).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			sb.append(" FROM ").append(table);
		} else {
			sb.append("SELECT * FROM ").append(table);
		}
		return sb.toString();
	}

	private JsonObject buildResults(int rows) {
		JsonObject result = new JsonObject();
		result.put("status", "ok");
		result.put("message", "");
		JsonArray fields = new JsonArray();
		JsonArray results = new JsonArray();
		result.put("fields", fields);
		result.put("results", results);
		result.put("rows", rows);
		return result;
	}

	private void transformResultSet(JsonArray results, ResultSet rs) throws SQLException{

		ResultSetMetaData rsmd = rs.getMetaData();
		int numColumns = rsmd.getColumnCount();

		while(rs.next()) {
			JsonArray row = new fr.wseduc.webutils.collections.JsonArray();
			results.add(row);
			for (int i = 1; i < numColumns + 1; i++) {
				switch (rsmd.getColumnType(i)) {
					case Types.NULL :
						row.add((Object) null);
						break;
					case Types.ARRAY:
						Array arr = rs.getArray(i);
						if(rs.wasNull()){
							row.add((Object) null);
						} else {
							ResultSet arrRs = arr.getResultSet();
							JsonArray jsonArray = new JsonArray();
							transformResultSet(jsonArray, arrRs);
							row.add(jsonArray);
						}
						break;
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
						long l = rs.getLong(i);
						if (rs.wasNull()) {
							row.add((Object) null);
						} else {
							row.add(l);
						}
						break;
					case Types.BIT:
						int precision = rsmd.getPrecision(i);
						if (precision != 1) {
							long l1 = rs.getLong(i);
							if (rs.wasNull()) {
								row.add((Object) null);
							} else {
								row.add(String.format("%0" + precision + "d", l1));
							}
						} else {
							boolean b = rs.getBoolean(i);
							if (rs.wasNull()) {
								row.add((Object) null);
							} else {
								row.add(b);
							}
						}
						break;
					case Types.BOOLEAN :
						boolean b = rs.getBoolean(i);
						if (rs.wasNull()) {
							row.add((Object) null);
						} else {
							row.add(b);
						}
						break;
					case Types.BLOB:
						row.add(rs.getBlob(i));
						break;
					case Types.FLOAT:
					case Types.REAL:
					case Types.DOUBLE:
						double d = rs.getDouble(i);
						if (rs.wasNull()) {
							row.add((Object) null);
						} else {
							row.add(d);
						}
						break;
					case Types.NVARCHAR:
					case Types.VARCHAR:
					case Types.LONGNVARCHAR:
					case Types.LONGVARCHAR:
						row.add(rs.getString(i));
						break;
					case Types.DATE:
						row.add(encode(rs.getDate(i)));
						break;
					case Types.TIMESTAMP:
						Timestamp t = rs.getTimestamp(i);
						if (rs.wasNull()) {
							row.add((Object) null);
						} else {
							row.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(t));
						}
						break;
					default:
						Object o = rs.getObject(i);
						if (o != null) {
							row.add(rs.getObject(i).toString());
						} else {
							row.add((Object) null);
						}
				}
			}
		}
	}

	private JsonObject buildResults(ResultSet rs) throws SQLException {
		JsonObject result = new JsonObject();
		result.put("status", "ok");
		result.put("message", "");
		JsonArray fields = new JsonArray();
		JsonArray results = new JsonArray();
		result.put("fields", fields);
		result.put("results", results);

		ResultSetMetaData rsmd = rs.getMetaData();
		int numColumns = rsmd.getColumnCount();
		for (int i = 1; i < numColumns + 1; i++) {
			fields.add(rsmd.getColumnName(i));
		}

		transformResultSet(results, rs);

		result.put("rows", results.size());
		return result;
	}

	private String escapeField(String str) {
		return "\"" + str.replace("\"", "\"\"") + "\"";
	}

	private String escapeValue(Object v) {
		if (v == null) {
			return "NULL";
		} else if (v instanceof Integer || v instanceof Boolean) {
			return v.toString();
		} else {
			return "'" + v.toString().replace("'", "''") + "'";
		}
	}

}
