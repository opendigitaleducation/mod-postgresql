package fr.wseduc.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.sql.*;

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
		conf.addDataSourceProperty("cachePrepStmts", "true");
		conf.addDataSourceProperty("prepStmtCacheSize", "250");
		conf.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		conf.addDataSourceProperty("useServerPrepStmts", "true");
		ds = new HikariDataSource(conf);

		vertx.eventBus().registerHandler(config.getString("address", "sql.persistor"), this);
	}

	@Override
	public void stop() {
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
		try {
			statement = connection.createStatement();
			if (statement.execute(query)) {
				resultSet = statement.getResultSet();
				return buildResults(resultSet);
			} else {
				return buildResults(statement.getUpdateCount());
			}
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
		JsonArray statements = message.body().getArray("statements");
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
			sendOK(message, new JsonObject().putArray("results", results));
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
		JsonArray values = json.getArray("values");
		if (query == null || query.isEmpty() || values == null) {
			return null;
		}
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(query);
			for (int i = 0; i < values.size(); i++) {
				statement.setObject(i + 1, values.get(i));
			}
			if (statement.execute()) {
				resultSet = statement.getResultSet();
				return buildResults(resultSet);
			} else {
				return buildResults(statement.getUpdateCount());
			}
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
		JsonArray fields = json.getArray("fields");
		JsonArray values = json.getArray("values");
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
		JsonArray fields = json.getArray("fields");
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
		result.putString("status", "ok");
		result.putString("message", "");
		JsonArray fields = new JsonArray();
		JsonArray results = new JsonArray();
		result.putArray("fields", fields);
		result.putArray("results", results);
		result.putNumber("rows", rows);
		return result;
	}

	private JsonObject buildResults(ResultSet rs) throws SQLException {
		JsonObject result = new JsonObject();
		result.putString("status", "ok");
		result.putString("message", "");
		JsonArray fields = new JsonArray();
		JsonArray results = new JsonArray();
		result.putArray("fields", fields);
		result.putArray("results", results);
		ResultSetMetaData rsmd = rs.getMetaData();
		int count = 0;
		while(rs.next()) {
			count++;
			int numColumns = rsmd.getColumnCount();
			JsonArray row = new JsonArray();
			results.add(row);
			for (int i = 1; i < numColumns + 1; i++) {

				if (count == 1) {
					String columnName = rsmd.getColumnName(i);
					fields.add(columnName);
				}

				if (rsmd.getColumnType(i)==java.sql.Types.ARRAY) {
					row.add(rs.getArray(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.BIGINT) {
					row.add(rs.getInt(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.BOOLEAN) {
					row.add(rs.getBoolean(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.BLOB) {
					row.add(rs.getBlob(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.DOUBLE) {
					row.add(rs.getDouble(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.FLOAT) {
					row.add(rs.getFloat(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.INTEGER) {
					row.add(rs.getInt(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.NVARCHAR) {
					row.add(rs.getNString(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.VARCHAR) {
					row.add(rs.getString(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.TINYINT) {
					row.add(rs.getInt(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.SMALLINT) {
					row.add(rs.getInt(i));
				} else if (rsmd.getColumnType(i)==java.sql.Types.DATE) {
					row.add(encode(rs.getDate(i)));
				} else if (rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP) {
					row.add(encode(rs.getTimestamp(i)));
				} else {
					row.add(rs.getObject(i).toString());
				}
			}
			result.putNumber("rows", count);
		}
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
