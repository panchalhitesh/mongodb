/*
 * Copyright 2007-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.mongodb.sink;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.util.JSON;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.Set;

/**
 * Implementation of {@link org.springframework.messaging.MessageHandler}
 * which writes Message payload into a MongoDb collection
 * identified by evaluation of the {@link #collectionNameExpression}.
 *
 * @author Hitesh Panchal
 *
 */
public class MongoDbStoringMessageHandler extends AbstractMessageHandler {

	public static String UNIQUE_FIELD_NAME = "unique_field_name";
	public static String UNIQUE_FIELD_VALUE = "unique_field_value";
	public static String OPERATION_TYPE = "op_type";

	Logger Logger = LoggerFactory.getLogger(MongoDbStoringMessageHandler.class);

	private volatile MongoOperations mongoTemplate;

	private volatile MongoDbFactory mongoDbFactory;

	private volatile MongoConverter mongoConverter;

	private volatile StandardEvaluationContext evaluationContext;

	private volatile Expression collectionNameExpression = new LiteralExpression("data");

	private volatile String uniqueFieldName;

	private volatile boolean initialized = false;
	
	/**
	 * Unique Field Name
	 * @return
	 */
	public String getUniqueFieldName() {
		return uniqueFieldName;
	}
	
	public Set<String> getUniqueFieldNames() {
		return Stream.of(this.uniqueFieldName.split(","))
				.map (elem -> new String(elem))
				.collect(Collectors.toSet());
	}

	/**
	 * Unique Field name
	 * @param uniqueFieldName
	 */
	public void setUniqueFieldName(String uniqueFieldName) {
		this.uniqueFieldName = uniqueFieldName;
	}

	/**
	 * Will construct this instance using provided {@link MongoDbFactory}
	 *
	 * @param mongoDbFactory The mongodb factory.
	 */
	public MongoDbStoringMessageHandler(MongoDbFactory mongoDbFactory) {
		Assert.notNull(mongoDbFactory, "'mongoDbFactory' must not be null");

		this.mongoDbFactory = mongoDbFactory;
	}

	/**
	 * Will construct this instance using fully created and initialized instance of
	 * provided {@link MongoOperations}
	 *
	 * @param mongoTemplate The MongoOperations implementation.
	 */
	public MongoDbStoringMessageHandler(MongoOperations mongoTemplate) {
		Assert.notNull(mongoTemplate, "'mongoTemplate' must not be null");

		this.mongoTemplate = mongoTemplate;
	}

	/**
	 * Allows you to provide custom {@link MongoConverter} used to assist in serialization
	 * of data written to MongoDb. Only allowed if this instance was constructed with a
	 * {@link MongoDbFactory}.
	 *
	 * @param mongoConverter The mongo converter.
	 */
	public void setMongoConverter(MongoConverter mongoConverter) {
		Assert.isNull(this.mongoTemplate,
				"'mongoConverter' can not be set when instance was constructed with MongoTemplate");
		this.mongoConverter = mongoConverter;
	}

	/**
	 * Sets the SpEL {@link Expression} that should resolve to a collection name
	 * used by {@link MongoOperations} to store data
	 *
	 * @param collectionNameExpression The collection name expression.
	 */
	public void setCollectionNameExpression(Expression collectionNameExpression) {
		Assert.notNull(collectionNameExpression, "'collectionNameExpression' must not be null");
		this.collectionNameExpression = collectionNameExpression;
	}

	@Override
	public String getComponentType() {
		return "mongo:outbound-channel-adapter";
	}

	@Override
	protected void onInit() {
		this.evaluationContext =
					ExpressionUtils.createStandardEvaluationContext(this.getBeanFactory());
		if (this.mongoTemplate == null) {
			this.mongoTemplate = new MongoTemplate(this.mongoDbFactory, this.mongoConverter);
		}
		this.initialized = true;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		Assert.isTrue(this.initialized, "This class is not yet initialized. Invoke its afterPropertiesSet() method");
		String collectionName = this.collectionNameExpression.getValue(this.evaluationContext, message, String.class);
		Assert.notNull(collectionName, "'collectionNameExpression' must not evaluate to null");
		Logger.info("Properties: uniqueFieldName: {}",uniqueFieldName);
		if(message.getHeaders().containsKey("op_type")){
			Object payload = message.getPayload();
			Logger.debug("Payload instance of {}",payload.getClass().getName());
			//perform operation based on the operation type
			switch(message.getHeaders().get("op_type").toString()){
				//update operations
				case "Insert":
				case "insert":
				case "INSERT":
				case "I":
				case "i":
					this.mongoTemplate.save(payload, collectionName);
					break;
				//update operations
				case "U":
				case "u":
				case "update":
				case "Update":
				case "UPDATE":
					BasicDBObject dbObject = BasicDBObject.parse(payload.toString());
					Set<String> keys = dbObject.keySet();
					Update updatedData = new Update();
					for (String key:keys) {
						if(!getUniqueFieldNames().contains(key))
							updatedData.set(key,dbObject.get(key));
					}
					Query updateQuery = new Query();
					for (String queryFieldName:getUniqueFieldNames()) {
						updateQuery.addCriteria(new Criteria(queryFieldName).is(dbObject.get(queryFieldName)));
					}
					Logger.debug("Updated records Query: {}",updateQuery.toString());
					Object result = this.mongoTemplate.updateMulti(updateQuery,updatedData,collectionName);
					Logger.info("Updated records counts: {}",result.toString());
				break;
				//Delete Operation
				case "D":
				case "d":
				case "DELETE":
				case "Delete":
				case "delete":
					BasicDBObject dbObjectRec = BasicDBObject.parse(payload.toString());
					Query deleteQuery = new Query();
					for (String queryFieldName:getUniqueFieldNames()) {
						deleteQuery.addCriteria(new Criteria(queryFieldName).is(dbObjectRec.get(queryFieldName)));
					}
					Logger.info("Delete object query {}",deleteQuery.toString());
					Object resultd = this.mongoTemplate.remove(deleteQuery, collectionName);
					Logger.info("Delete records counts: {}",resultd.toString());
				break;
				default:
					this.mongoTemplate.save(payload, collectionName);
				break;

			}
		}else {
			Object payload = message.getPayload();
			this.mongoTemplate.save(payload, collectionName);
		}
	}
}
