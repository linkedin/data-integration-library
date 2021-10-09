// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import org.apache.gobblin.configuration.State;

import static com.linkedin.cdi.configuration.StaticConstants.*;

public interface PropertyCollection {

  IntegerProperties MSTAGE_ABSTINENT_PERIOD_DAYS = new IntegerProperties("ms.abstinent.period.days") {
    @Override
    public Long getMillis(State state) {
      return 24L * 3600L * 1000L * this.getValidNonblankWithDefault(state);
    }
  };

  JsonObjectProperties MSTAGE_ACTIVATION_PROPERTY = new JsonObjectProperties("ms.activation.property");
  JsonObjectProperties MSTAGE_AUTHENTICATION = new JsonObjectProperties("ms.authentication") {
    @Override
    public boolean isValid(State state) {
      if (super.isValid(state) && !super.isBlank(state)) {
        JsonObject auth = GSON.fromJson(state.getProp(getConfig()), JsonObject.class);
        return auth.entrySet().size() > 0 && auth.has("method") && auth.has("encryption");
      }
      return super.isValid(state);
    }
  };

  BooleanProperties MSTAGE_BACKFILL = new BooleanProperties("ms.backfill", Boolean.FALSE);
  LongProperties MSTAGE_CALL_INTERVAL_MILLIS = new LongProperties("ms.call.interval.millis");
  StringProperties MSTAGE_CONNECTION_CLIENT_FACTORY = new StringProperties("ms.connection.client.factory",
          "com.linkedin.cdi.factory.DefaultConnectionClientFactory");
  LongProperties MSTAGE_CONVERTER_CSV_MAX_FAILURES = new LongProperties("ms.converter.csv.max.failures");
  BooleanProperties MSTAGE_CONVERTER_KEEP_NULL_STRINGS = new BooleanProperties("ms.converter.keep.null.strings", Boolean.FALSE);
  BooleanProperties MSTAGE_CSV_COLUMN_HEADER = new BooleanProperties("ms.csv.column.header", Boolean.FALSE);
  IntegerProperties MSTAGE_CSV_COLUMN_HEADER_INDEX = new IntegerProperties("ms.csv.column.header.index");
  StringProperties MSTAGE_CSV_COLUMN_PROJECTION = new StringProperties("ms.csv.column.projection") {
    @Override
    public boolean isValid(State state) {
      if (super.isValid(state) && !super.isBlank(state)) {
        String columnProjections = state.getProp(getConfig());
        return columnProjections != null && columnProjections.split(KEY_WORD_COMMA).length > 0;
      }
      return super.isValid(state);
    }
  };

  StringProperties MSTAGE_CSV_DEFAULT_FIELD_TYPE = new StringProperties("ms.csv.default.field.type");
  StringProperties MSTAGE_CSV_ESCAPE_CHARACTER = new StringProperties("ms.csv.escape.character", "u005C");
  StringProperties MSTAGE_CSV_QUOTE_CHARACTER = new StringProperties("ms.csv.quote.character", "\"");
  StringProperties MSTAGE_CSV_SEPARATOR = new StringProperties("ms.csv.separator", KEY_WORD_COMMA);
  IntegerProperties MSTAGE_CSV_SKIP_LINES = new IntegerProperties("ms.csv.skip.lines");
  BooleanProperties MSTAGE_DATA_EXPLICIT_EOF = new BooleanProperties("ms.data.explicit.eof", Boolean.FALSE);
  JsonObjectProperties MSTAGE_DATA_DEFAULT_TYPE = new JsonObjectProperties("ms.data.default.type");
  StringProperties MSTAGE_DATA_FIELD = new StringProperties("ms.data.field");
  JsonArrayProperties MSTAGE_DERIVED_FIELDS = new JsonArrayProperties("ms.derived.fields") {
    @Override
    public boolean isValid(State state) {
      if (super.isValid(state) && !isBlank(state)) {
        // Derived fields should meet general JsonArray configuration requirements
        // and contain only JsonObject items that each has a "name" element and a "formula" element
        JsonArray derivedFields = GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
        for (JsonElement field : derivedFields) {
          if (!field.isJsonObject()
              || !field.getAsJsonObject().has(KEY_WORD_NAME)
              || !field.getAsJsonObject().has(KEY_WORD_FORMULA)) {
            return false;
          }
        }
      }
      return super.isValid(state);
    }
  };

  BooleanProperties MSTAGE_ENABLE_CLEANSING = new BooleanProperties("ms.enable.cleansing", Boolean.TRUE);
  BooleanProperties MSTAGE_ENABLE_DYNAMIC_FULL_LOAD = new BooleanProperties("ms.enable.dynamic.full.load", Boolean.TRUE);
  BooleanProperties MSTAGE_ENABLE_SCHEMA_BASED_FILTERING = new BooleanProperties("ms.enable.schema.based.filtering", Boolean.TRUE);
  JsonArrayProperties MSTAGE_ENCRYPTION_FIELDS = new JsonArrayProperties("ms.encryption.fields");
  StringProperties MSTAGE_EXTRACTOR_CLASS = new StringProperties("ms.extractor.class");
  StringProperties MSTAGE_EXTRACTOR_TARGET_FILE_NAME = new StringProperties("ms.extractor.target.file.name");
  StringProperties MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION = new StringProperties("ms.extractor.target.file.permission", "755");
  StringProperties MSTAGE_EXTRACT_PREPROCESSORS = new StringProperties("ms.extract.preprocessors");
  JsonObjectProperties MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS = new JsonObjectProperties("ms.extract.preprocessor.parameters");
  IntegerProperties MSTAGE_GRACE_PERIOD_DAYS = new IntegerProperties("ms.grace.period.days") {
    @Override
    public Long getMillis(State state) {
      return 24L * 3600L * 1000L * this.getProp(state);
    }
  };

  JsonObjectProperties MSTAGE_HTTP_REQUEST_HEADERS = new JsonObjectProperties("ms.http.request.headers");
  StringProperties MSTAGE_HTTP_REQUEST_METHOD = new StringProperties("ms.http.request.method");
  JsonObjectProperties MSTAGE_HTTP_RESPONSE_TYPE = new JsonObjectProperties("ms.http.response.type");
  JsonObjectProperties MSTAGE_HTTP_STATUSES = new JsonObjectProperties("ms.http.statuses",
          GSON.fromJson("{\"success\":[200,201,202], \"pagination_error\":[401]}", JsonObject.class));
  JsonObjectProperties MSTAGE_HTTP_STATUS_REASONS = new JsonObjectProperties("ms.http.status.reasons");
  StringProperties MSTAGE_JDBC_SCHEMA_REFACTOR = new StringProperties("ms.jdbc.schema.refactor", "none");
  StringProperties MSTAGE_JDBC_STATEMENT = new StringProperties("ms.jdbc.statement");
  StringProperties MSTAGE_KAFKA_BROKERS = new StringProperties("ms.kafka.brokers");
  StringProperties MSTAGE_KAFKA_SCHEMA_REGISTRY_URL = new StringProperties("ms.kafka.schema.registry.url");
  StringProperties MSTAGE_KAFKA_CLIENT_ID = new StringProperties("ms.kafka.clientId");
  StringProperties MSTAGE_KAFKA_TOPIC_NAME = new StringProperties("ms.kafka.audit.topic.name");
  LongProperties MSTAGE_NORMALIZER_BATCH_SIZE = new LongProperties("ms.normalizer.batch.size", 500L);
  JsonArrayProperties MSTAGE_OUTPUT_SCHEMA = new JsonArrayProperties("ms.output.schema");
  JsonObjectProperties MSTAGE_PAGINATION = new JsonObjectProperties("ms.pagination");
  JsonArrayProperties MSTAGE_PARAMETERS = new JsonArrayProperties("ms.parameters") {
    @Override
    public boolean isValid(State state) {
      if (super.isValid(state) && !isBlank(state)) {
        // Derived fields should meet general JsonArray configuration requirements
        // and contain only JsonObject items that each has a "name" element and a "formula" element
        JsonArray parameters = GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
        return parameters.size() > 0;
      }
      return super.isValid(state);
    }
  };

  JsonArrayProperties MSTAGE_PAYLOAD_PROPERTY = new JsonArrayProperties("ms.payload.property");
  JsonObjectProperties MSTAGE_RETENTION =
      new JsonObjectProperties("ms.retention") {
        @Override
        public JsonObject getDefaultValue() {
          JsonObject retention = new JsonObject();
          retention.addProperty("state.store", "P90D"); // keep 90 days state store by default
          retention.addProperty("publish.dir", "P731D"); // keep 2 years published data
          retention.addProperty("log", "P30D");
          return retention;
        }
      };

  IntegerProperties MSTAGE_S3_LIST_MAX_KEYS = new IntegerProperties("ms.s3.list.max.keys",1000);
  JsonObjectProperties MSTAGE_SCHEMA_CLENSING = new JsonObjectProperties("ms.schema.cleansing");
  JsonArrayProperties MSTAGE_SECONDARY_INPUT = new JsonArrayProperties("ms.secondary.input");
  JsonObjectProperties MSTAGE_SESSION_KEY_FIELD = new JsonObjectProperties("ms.session.key.field");
  StringProperties MSTAGE_SOURCE_DATA_CHARACTER_SET = new StringProperties("ms.source.data.character.set",
      StandardCharsets.UTF_8.toString());

  StringProperties MSTAGE_SOURCE_FILES_PATTERN = new StringProperties("ms.source.files.pattern", REGEXP_DEFAULT_PATTERN);
  JsonObjectProperties MSTAGE_SOURCE_S3_PARAMETERS = new JsonObjectProperties("ms.source.s3.parameters");
  StringProperties MSTAGE_SOURCE_SCHEMA_URN = new StringProperties("ms.source.schema.urn");
  StringProperties MSTAGE_SOURCE_URI = new StringProperties("ms.source.uri");
  JsonArrayProperties MSTAGE_TARGET_SCHEMA = new JsonArrayProperties("ms.target.schema");
  StringProperties MSTAGE_TARGET_SCHEMA_URN = new StringProperties("ms.target.schema.urn");
  StringProperties MSTAGE_TOTAL_COUNT_FIELD = new StringProperties("ms.total.count.field");
  JsonObjectProperties MSTAGE_VALIDATION_ATTRIBUTES =
      new JsonObjectProperties("ms.validation.attributes") {
        @Override
        public JsonObject getDefaultValue() {
          JsonObject attributesJson = new JsonObject();
          attributesJson.addProperty(StaticConstants.KEY_WORD_THRESHOLD, 0);
          attributesJson.addProperty(StaticConstants.KEY_WORD_CRITERIA, StaticConstants.KEY_WORD_FAIL);
          return attributesJson;
        }
      };

  LongProperties MSTAGE_WAIT_TIMEOUT_SECONDS = new LongProperties("ms.wait.timeout.seconds", 600L) {
    @Override
    public Long getMillis(State state) {
      return 1000L * this.getValidNonblankWithDefault(state);
    }
  };

  JsonArrayProperties MSTAGE_WATERMARK = new JsonArrayProperties("ms.watermark");
  JsonArrayProperties MSTAGE_WATERMARK_GROUPS = new JsonArrayProperties("ms.watermark.groups");
  LongProperties MSTAGE_WORKUNIT_STARTTIME_KEY= new LongProperties("ms.work.unit.scheduling.starttime");
  LongProperties MSTAGE_WORK_UNIT_MIN_RECORDS = new LongProperties("ms.work.unit.min.records");
  LongProperties MSTAGE_WORK_UNIT_MIN_UNITS = new LongProperties("ms.work.unit.min.units");
  IntegerProperties MSTAGE_WORK_UNIT_PACING_SECONDS = new IntegerProperties("ms.work.unit.pacing.seconds") {
    @Override
    public Long getMillis(State state) {
      return 1000L * this.getProp(state);
    }
  };

  IntegerProperties MSTAGE_WORK_UNIT_PARALLELISM_MAX = new IntegerProperties("ms.work.unit.parallelism.max") {
    @Override
    public boolean isValid(State state) {
      if (super.isValid(state) && !super.isBlank(state)) {
        Integer parallelMax = state.getPropAsInt(getConfig());
        return parallelMax > 0;
      }
      return super.isValid(state);
    }
  };

  BooleanProperties MSTAGE_WORK_UNIT_PARTIAL_PARTITION =
      new BooleanProperties("ms.work.unit.partial.partition", Boolean.TRUE);
  StringProperties MSTAGE_WORK_UNIT_PARTITION = new StringProperties("ms.work.unit.partition", "none");
  StringProperties CONVERTER_CLASSES = new StringProperties("converter.classes");
  StringProperties DATA_PUBLISHER_FINAL_DIR = new StringProperties("data.publisher.final.dir");
  StringProperties DATASET_URN_KEY = new StringProperties("dataset.urn");
  StringProperties ENCRYPT_KEY_LOC = new StringProperties("encrypt.key.loc");
  StringProperties EXTRACTOR_CLASSES = new StringProperties("extractor.class");
  // add a default value of FALSE to Gobblin configuration extract.is.full
  BooleanProperties EXTRACT_IS_FULL = new BooleanProperties("extract.is.full", Boolean.FALSE);
  StringProperties EXTRACT_NAMESPACE_NAME_KEY = new StringProperties("extract.namespace");
  StringProperties EXTRACT_TABLE_NAME_KEY = new StringProperties("extract.table.name");
  StringProperties EXTRACT_TABLE_TYPE_KEY = new StringProperties("extract.table.type", "SNAPSHOT_ONLY") {
    @Override
    public String getValidNonblankWithDefault(State state) {
      return super.getValidNonblankWithDefault(state).toUpperCase();
    }
  };

  StringProperties SOURCE_CLASS = new StringProperties("source.class");
  StringProperties SOURCE_CONN_USERNAME = new StringProperties("source.conn.username");
  StringProperties SOURCE_CONN_PASSWORD = new StringProperties("source.conn.password");
  StringProperties SOURCE_CONN_USE_PROXY_URL = new StringProperties("source.conn.use.proxy.url");
  StringProperties SOURCE_CONN_USE_PROXY_PORT = new StringProperties("source.conn.use.proxy.port");
  BooleanProperties STATE_STORE_ENABLED = new BooleanProperties("state.store.enabled", Boolean.TRUE);
}
