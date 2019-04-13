/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.mongodb.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.data.mongo.AutoConfigureDataMongo;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.integration.mongodb.store.MessageDocument;
import org.springframework.integration.mongodb.support.BinaryToMessageConverter;
import org.springframework.integration.mongodb.support.MessageToBinaryConverter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.MutableMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Artem Bilan
 * @author Chris Schaefer
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@AutoConfigureDataMongo
@DirtiesContext
public abstract class MongoDbSinkApplicationTests {

	@Autowired
	protected MongoTemplate mongoTemplate;

	@Autowired
	protected Sink sink;

	@Autowired
	protected MongoDbSinkProperties mongoDbSinkProperties;

	@TestPropertySource(properties = "mongodb.collection=testing")
	static public class CollectionNameTests extends MongoDbSinkApplicationTests {

		@Test
		public void test() {
			Map<String, String> data1 = new HashMap<>();
			data1.put("foo", "bar");

			Map<String, String> data2 = new HashMap<>();
			data2.put("firstName", "Foo");
			data2.put("lastName", "Bar");

			Map<String, Object> updateHeaders = new HashMap<>();
			updateHeaders.put(MongoDbStoringMessageHandler.OPERATION_TYPE, "U");
			this.sink.input().send(new GenericMessage<>(data1));
			this.sink.input().send(new GenericMessage<>(data2));
			this.sink.input().send(new GenericMessage<>("{\"my_data1\": \"THE DATA\"}"));
			this.sink.input().send(new GenericMessage<>("{\"my_data2\": \"THE DATA\",\"uniqueId\": 123456789,\"my_data3\": \"THE DATA\"}"));
			this.sink.input().send(new GenericMessage<>("{\"my_data\": \"THE DATA\"}".getBytes()));

			List<Document> result =
					this.mongoTemplate.findAll(Document.class, mongoDbSinkProperties.getCollection());

			assertEquals(5, result.size());

			Document dbObject = result.get(0);
			Logger.info("$$$$ Message 0 Object Size: "+dbObject.size()+" BSon String: "+dbObject.toString());
			assertNotNull(dbObject.get("_id"));
			assertEquals(dbObject.get("foo"), "bar");

			dbObject = result.get(1);
			Logger.info("$$$$ Message 1  Object Size: "+dbObject.size()+" BSon String: "+dbObject.toString());
			assertEquals(dbObject.get("firstName"), "Foo");
			assertEquals(dbObject.get("lastName"), "Bar");

			dbObject = result.get(2);
			assertNull(dbObject.get("_class"));
			Logger.info("$$$$ Message 2 Object Size: "+dbObject.size()+" BSon String: "+dbObject.toString());
			assertEquals(dbObject.get("my_data1"), "THE DATA");

			dbObject = result.get(3);
			assertNull(dbObject.get("_class"));
			Logger.info("$$$$ Message 3 Object Size: "+dbObject.size()+" BSon String: "+dbObject.toString());
			//assertEquals(dbObject.get("uniqueId"), 123456789);

			dbObject = result.get(4);
			assertNull(dbObject.get("_class"));
			Logger.info("$$$$ Message 4 Object Size: "+dbObject.size()+" BSon String: "+dbObject.toString());
			assertEquals(dbObject.get("my_data"), "THE DATA");

			//update operation
			this.sink.input().send(new GenericMessage<>("{\"uniqueId\": 123456789,\"my_data2\": \"updated data\",\"my_data3\": \"THE DATA3\"}",updateHeaders));
			List<Document> result2 =
					this.mongoTemplate.findAll(Document.class, mongoDbSinkProperties.getCollection());
			assertEquals(5, result2.size());
			dbObject = result2.get(3);
			assertNull(dbObject.get("_class"));
			Logger.info("$$$$ Update Message 4 Object Size: "+dbObject.size()+" BSon String: "+dbObject.toString());
			assertEquals(dbObject.get("my_data2"), "updated data");
			assertEquals(dbObject.get("my_data3"), "THE DATA3");
			assertEquals(dbObject.get("uniqueId"), 123456789);
			//delete operation
			Map<String, Object> deleteHeaders = new HashMap<>();
			deleteHeaders.put(MongoDbStoringMessageHandler.OPERATION_TYPE, "D");
			this.sink.input().send(new GenericMessage<>("{\"uniqueId\": 123456789}",deleteHeaders));
			List<Document> result3 =
					this.mongoTemplate.findAll(Document.class, mongoDbSinkProperties.getCollection());
			assertEquals(4, result3.size());
		}


	}

	@TestPropertySource(properties = "mongodb.collection-expression=headers.collection")
	static public class CollectionExpressionStoreMessageTests extends MongoDbSinkApplicationTests {

		@Test
		@SuppressWarnings("rawtypes")
		public void test() {
			Message<String> mutableMessage = MutableMessageBuilder.withPayload("foo")
					.setHeader("test", "1")
					.build();

			this.sink.input()
					.send(MessageBuilder.withPayload(new MessageDocument(mutableMessage))
							.setHeader("collection", "testing2")
							.build());

			List<MessageDocument> result = this.mongoTemplate.findAll(MessageDocument.class, "testing2");

			assertEquals(1, result.size());
			Message<?> message = result.get(0).getMessage();
			assertEquals(mutableMessage, message);
		}

	}

	@SpringBootApplication
	@EntityScan(basePackageClasses = MessageDocument.class)
	public static class MongoSinkApplication {

		@Bean
		public MongoCustomConversions customConversions() {
			List<Object> customConverters = new ArrayList<>();
			customConverters.add(new MessageToBinaryConverter());
			customConverters.add(new BinaryToMessageConverter());
			return new MongoCustomConversions(customConverters);
		}

	}

}
