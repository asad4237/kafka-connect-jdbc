/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.lang.model.util.ElementScanner6;
import javax.ws.rs.NotFoundException;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

//import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
//import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
//import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
//import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSchema;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs);
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

  private Schema inferSchema(String topic, JsonNode objNode) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(topic);

    Iterator<Map.Entry<String, JsonNode>> nodes = objNode.fields();

    while (nodes.hasNext()) {
      Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) nodes.next();
      Schema fieldType;
      if (entry.getValue().isBoolean()) {
        fieldType = Schema.BOOLEAN_SCHEMA;
      } else if (entry.getValue().isInt()) {
        fieldType = Schema.INT32_SCHEMA;
      } else if (entry.getValue().isBigInteger()) {
        fieldType = Schema.INT64_SCHEMA;
      } else if (entry.getValue().isFloat() || entry.getValue().isDouble()) {
        fieldType = Schema.FLOAT32_SCHEMA;
      } else if (entry.getValue().isBigDecimal()) {
        fieldType = Schema.FLOAT64_SCHEMA;
      } else if (entry.getValue().isNull()) {
        fieldType = Schema.OPTIONAL_STRING_SCHEMA;
      }else {
        // handle all other types including nested objects
        fieldType = Schema.STRING_SCHEMA;
      }
      schemaBuilder = schemaBuilder.field(entry.getKey(), fieldType);
      // logger.info("key --> " + entry.getKey() + " value-->" + entry.getValue());
    }
    Schema schema = schemaBuilder.build();
    return schema;
  }

  void HandleCustomSchema(SinkRecord record,BufferedRecords buffer)
  {
    try (JsonConverter converter = new JsonConverter()) {

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode actualObj = null;
      Map<String, String> map = new HashMap<>();
      map.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
      map.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
      converter.configure(map);

      final ObjectWriter writer = objectMapper.writer();

      
      //no converter is provided from the config so we have to deal with the string json.
      if (record.value() instanceof String) {
        String value = (String) record.value();
        actualObj = objectMapper.readTree(value);
        if (!actualObj.isArray())
        {
          ArrayNode list = JSON_NODE_FACTORY.arrayNode();
          list.add(actualObj);
          actualObj = list;
        }

      } 
      //json converter was specified in the config so we expect an array object.
      else if (record.value() instanceof ArrayList) {
        final byte[] bytes = converter.fromConnectData(record.topic(),null,record.value());
        JsonNode envelope = objectMapper.readTree(bytes);
        actualObj = envelope.get("payload");

      } 
      //json converter was specified in the config so we expect an object.
      else if (record.value() instanceof HashMap) {
        ArrayList list = new ArrayList();
        list.add(record.value());
        final byte[] bytes = converter.fromConnectData(record.topic(),null,list);
        JsonNode envelope = objectMapper.readTree(bytes);
        actualObj = envelope.get("payload");

      } else {
        throw new Exception(String.format("received unexpected JSON. custom.json.enable is true  %s",record.value()));
      }
      ObjectNode jsonSchema = null;
      
      try
      {
        jsonSchema = (ObjectNode) objectMapper.readTree(config.customJSONSchema());
      }
      catch(Exception e){
        log.error("Exception: error parsing schema json, will revert back to schema inference: {}", config.customJSONSchema(), e);
      } 
      
      for (final JsonNode jsonNode : actualObj) {

        if (!jsonNode.isNull() && jsonNode.isObject()) {

          if(jsonSchema == null)
          {
            Schema schema = inferSchema(record.topic(), jsonNode);
            jsonSchema = converter.asJsonSchema(schema);
            log.debug("Schema is not provided or was invalid so inferring schema from the data: " + jsonSchema.toPrettyString());
          }

          ObjectNode jsonWithSchemaEnvelope = JSON_NODE_FACTORY.objectNode();
          jsonWithSchemaEnvelope.set("schema", jsonSchema);
          jsonWithSchemaEnvelope.set("payload", jsonNode);

          final byte[] jsonWithSchemaEnvelopeBytes = writer.writeValueAsBytes(jsonWithSchemaEnvelope);

          SchemaAndValue valueAndSchema = converter.toConnectData(record.topic(), jsonWithSchemaEnvelopeBytes);

          SinkRecord sinkRecord = new SinkRecord(record.topic(), record.kafkaPartition(),
              record.keySchema(), record.key(),
              valueAndSchema.schema(), valueAndSchema.value(),
              record.kafkaOffset(),
              record.timestamp(),
              record.timestampType(),
              record.headers());

          buffer.add(sinkRecord);

        } else {
          throw new Exception(
              String.format("Received unexpected JSON. custom.json.enable is true. either node is null or it is not a object %s",actualObj));
        }
        // Use the writer for thread safe access.

      }
    } catch (Exception ex) {
      log.error("Exception: error while processing received json: {}", record.value(), ex);
      throw new ConnectException(String.format("Exception: error while processing received json: {}", record.value()), ex);
    }
  }
  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        final TableId tableId = destinationTable(record.topic());
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }

        if (config.customJsonEnabled) {
          if(config.customJsonEnabled && config.schemasEnabled)
          {
            throw new ConnectException("Make a careful choice, schema registry and custom schema both cannot be enabled at a time");
          }
          HandleCustomSchema(record,buffer);
        } else {
          buffer.add(record);
        }

      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
