package fr.wseduc.sql;

import com.github.mauricio.async.db.*;
import com.github.mauricio.async.db.pool.ConnectionPool;
import com.github.mauricio.async.db.pool.PoolConfiguration$;
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection;
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory;
import com.github.mauricio.async.db.postgresql.util.URLParser;
import com.github.mauricio.async.db.util.ExecutorServiceUtils$;
import com.github.mauricio.async.db.util.NettyUtils$;
import io.netty.util.CharsetUtil;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;
import scala.collection.Seq;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SqlPersistor extends BusModBase implements Handler<Message<JsonObject>> {

	private ConnectionPool<PostgreSQLConnection> pool;

	@Override
	public void start() {
		super.start();
		String url = config.getString("url", "jdbc:postgresql://localhost:5432/testdb?user=postgres");
		Configuration configuration = URLParser.parse(url, CharsetUtil.UTF_8);
		PostgreSQLConnectionFactory factory = new PostgreSQLConnectionFactory(
				configuration,
				NettyUtils$.MODULE$.DefaultEventLoopGroup(),
				ExecutorServiceUtils$.MODULE$.CachedExecutionContext()
		);
		pool = new ConnectionPool<>(
				factory,
				PoolConfiguration$.MODULE$.Default(),
				ExecutorServiceUtils$.MODULE$.CachedExecutionContext()
		);
		vertx.eventBus().registerHandler(config.getString("address", "sql.persistor"), this);
	}

	@Override
	public void stop() {
		super.stop();
		if (pool != null) {
			pool.close();
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
		Future<QueryResult> future = raw(message.body());
		if (future != null) {
			reply(message, future);
		} else {
			sendError(message, "invalid.query");
		}
	}

	private Future<QueryResult> raw(JsonObject json) {
		String query = json.getString("command");
		if (query == null || query.isEmpty()) {
			return null;
		}
		return raw(query);
	}

	private Future<QueryResult> raw(String query) {
		return pool.sendQuery(query);
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
		PostgreSQLConnection connection = null;
		try {
			connection = sync(pool.take());
			sync(connection.sendQuery("BEGIN;"));
			JsonArray results = new JsonArray();
			for (Object s : statements) {
				if (!(s instanceof JsonObject)) continue;
				JsonObject json = (JsonObject) s;
				String action = json.getString("action", "");
				switch (action) {
					case "insert":
						results.add(buildResults(sync(raw(insertQuery(json)))));
						break;
					case "select":
						results.add(buildResults(sync(raw(selectQuery(json)))));
						break;
					case "raw":
						results.add(buildResults(sync(raw(json))));
						break;
					case "prepared":
						results.add(buildResults(sync(prepared(json))));
						break;
					default:
						sync(connection.sendQuery("ROLLBACK;"));
						throw new IllegalArgumentException("invalid.action");
				}
			}
			sync(connection.sendQuery("COMMIT;"));
			sendOK(message, new JsonObject().putArray("results", results));
		} catch (Exception e) {
			sendError(message, e.getMessage(), e);
		} finally {
			if (connection != null) {
				pool.giveBack(connection);
			}
		}
	}

	private void doPrepared(Message<JsonObject> message) {
		Future<QueryResult> future = prepared(message.body());
		if (future != null) {
			reply(message, future);
		} else {
			sendError(message, "invalid.query");
		}
	}

	private Future<QueryResult> prepared(JsonObject json) {
		String query = json.getString("statement");
		JsonArray values = json.getArray("values");
		if (query == null || query.isEmpty() || values == null) {
			return null;
		}
		return pool.sendPreparedStatement(query, convert(values.toList()));
	}

	private void doInsert(Message<JsonObject> message) {
		JsonObject json = message.body();
		String query = insertQuery(json);
		if (query == null) {
			sendError(message, "invalid.query");
			return;
		}
		Future<QueryResult> future = pool.sendQuery(query);
		reply(message, future);
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
		Future<QueryResult> future = pool.sendQuery(query);
		reply(message, future);
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

	private void reply(Message<JsonObject> message, Future<QueryResult> future) {
		try {
			QueryResult result = Await.result(future, Duration.apply(5, TimeUnit.SECONDS));
			sendOK(message, buildResults(result));
		} catch (Exception e) {
			sendError(message, "database.error", e);
		}
	}

	private JsonObject buildResults(QueryResult qr) {
		JsonObject result = new JsonObject();
		result.putString("status", "ok");
		result.putString("message", qr.statusMessage());
		result.putNumber("rows", qr.rowsAffected());
		JsonArray fields = new JsonArray();
		JsonArray results = new JsonArray();
		result.putArray("fields", fields);
		result.putArray("results", results);

		if (qr.rows().isDefined()) {
			ResultSet resultSet = qr.rows().get();
			for (String f : convert(resultSet.columnNames())) {
				fields.add(f);
			}
			for (RowData rowData : convert(resultSet)) {
				results.add(rowDataToJsonArray(rowData));
			}
		}
		return result;
	}

	private Object dataToJson(Object data) {
		if (data == null) {
			return null;
		} else if (!(data instanceof scala.Boolean) &&
				!(data instanceof Number) && !(data instanceof String) &&
				!(data instanceof Byte[]) && !(data instanceof JsonElement)) {
			return data.toString();
		} else {
			return data;
		}
	}

	private JsonArray rowDataToJsonArray(RowData rowData) {
		JsonArray values = new JsonArray();
		for (Object o: convert(rowData)) {
			values.add(dataToJson(o));
		}
		return values;
	}

	private <T> List<T> convert(Seq<T> seq) {
		return scala.collection.JavaConversions.seqAsJavaList(seq);
	}

	private <T> Seq<T> convert(List<T> list) {
		return scala.collection.JavaConversions.asScalaBuffer(list).toSeq();
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

	private <T> T sync(Future<T> future) throws Exception {
		return Await.result(future, Duration.apply(5, TimeUnit.SECONDS));
	}

}
