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

package fr.wseduc.sql.test.integration.java;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

public class SqlPersistorTest extends TestVerticle {

	public static final String PERSISTOR_ADDRESS = "sql.persistor";

	@Override
	public void start() {
		JsonObject config = new JsonObject();
		config.putString("address", PERSISTOR_ADDRESS);
		config.putString("url", "jdbc:postgresql://localhost:5432/test");
		config.putString("username", "web-education");
		config.putString("password", "We_1234");
		container.deployModule(System.getProperty("vertx.modulename"), config, new AsyncResultHandler<String>() {
			@Override
			public void handle(AsyncResult<String> ar) {
				if (ar.succeeded()) {
					SqlPersistorTest.super.start();
				} else {
					ar.cause().printStackTrace();
				}
			}
		});
	}

	@Test
	public void testInsertReturn()  {
		String q =  "INSERT INTO test.tests (name,number,owner) VALUES " +
				"('paper',3,'a6930a8f-d5cc-4968-9208-5251210f99bd') RETURNING id";
//		String q = "CREATE TABLE pff (" +
//				"id bigserial NOT NULL PRIMARY KEY, " +
//				"username VARCHAR(255)" +
//				");";
	//	String q = "INSERT INTO pff (username) VALUES ('aïe') RETURNING id";
//		String q = "Select * from test.tests";
		JsonObject raw = new JsonObject()
				.putString("command", q)
				.putString("action", "raw");
		vertx.eventBus().send(PERSISTOR_ADDRESS, raw, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> message) {
				System.out.println(message.body().encodePrettily());
				testComplete();
			}
		});
	}

	@Test
	public void testTransaction() {
		JsonArray statements = new JsonArray();
		String q =  "INSERT INTO test.tests (name,number,owner) VALUES " +
				"('paper',3,'a6930a8f-d5cc-4968-9208-5251210f99bd')";
		JsonObject raw = new JsonObject()
				.putString("command", q)
				.putString("action", "raw");
		JsonObject insert = new JsonObject()
				.putString("table", "test.tests")
				.putArray("fields", new JsonArray().add("name").add("number").add("owner"))
				.putArray("values", new JsonArray()
					.add(new JsonArray().add("blip").add(42).add("a6930a8f-d5cc-4968-9208-5251210f99bd"))
					.add(new JsonArray().add("blop").add(51).add("a6930a8f-d5cc-4968-9208-5251210f99bd"))
					.add(new JsonArray().add("bla").add(27).add("a6930a8f-d5cc-4968-9208-5251210f99bd"))
				)
				.putString("returning", "id")
				.putString("action", "insert");
		JsonObject preparedS = new JsonObject()
				.putString("statement", "SELECT * FROM test.tests WHERE name = ?")
				.putArray("values", new JsonArray().add("bla"))
				.putString("action", "prepared");
		JsonObject prepared = new JsonObject()
				.putString("statement", "UPDATE test.tests SET number = ?, name = ? WHERE name = ?")
				.putArray("values", new JsonArray().add(99).add("modified").add("bla"))
				.putString("action", "prepared");
		JsonObject select = new JsonObject()
				.putString("table", "test.tests")
				.putArray("fields", new JsonArray().add("id").add("name").add("number"))
				.putString("action", "select");
		statements.add(raw).add(insert).add(preparedS).add(prepared).add(select);
		JsonObject json = new JsonObject()
				.putString("action", "transaction")
				.putArray("statements", statements);
		vertx.eventBus().send(PERSISTOR_ADDRESS, json, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> message) {
				System.out.println(message.body().encodePrettily());
				testComplete();
			}
		});
	}

}
