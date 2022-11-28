// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.cdi.extractor.FileDumpExtractor;
import com.linkedin.cdi.util.SchemaUtils;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.gobblin.configuration.State;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * Defines all properties here.
 *
 * Properties can have their unique default values and validation rules through
 * inline class override. If the rules become too complicated, a new class
 * should be derived to avoid making this class too clumsy.
 *
 * Definitions are recommended to be organized in alphabetic order.
 *
 */
public interface PropertyCollection {

  // default: 0, minimum: 0, maximum: -
  IntegerProperties MSTAGE_ABSTINENT_PERIOD_DAYS = new IntegerProperties("ms.abstinent.period.days") {
    @Override
    public Long getMillis(State state) {
      return 24L * 3600L * 1000L * this.get(state);
    }
  };

  JsonObjectProperties MSTAGE_ACTIVATION_PROPERTY = new JsonObjectProperties("ms.activation.property");
  AuthenticationProperties MSTAGE_AUTHENTICATION = new AuthenticationProperties("ms.authentication");
  BooleanProperties MSTAGE_BACKFILL = new BooleanProperties("ms.backfill", Boolean.FALSE);

  // default: 0, minimum: 0, maximum: -
  LongProperties MSTAGE_CALL_INTERVAL_MILLIS = new LongProperties("ms.call.interval.millis");

  StringProperties MSTAGE_CONNECTION_CLIENT_FACTORY = new StringProperties("ms.connection.client.factory",
          "com.linkedin.cdi.factory.DefaultConnectionClientFactory");


  CsvProperties MSTAGE_CSV = new CsvProperties("ms.csv");

  BooleanProperties MSTAGE_DATA_EXPLICIT_EOF = new BooleanProperties("ms.data.explicit.eof", Boolean.FALSE);
  JsonObjectProperties MSTAGE_DATA_DEFAULT_TYPE = new JsonObjectProperties("ms.data.default.type");
  StringProperties MSTAGE_DATA_FIELD = new StringProperties("ms.data.field");
  DerivedFieldsProperties MSTAGE_DERIVED_FIELDS = new DerivedFieldsProperties("ms.derived.fields");

  /**
   * Deprecated
   * @see #MSTAGE_SCHEMA_CLEANSING
   */
  BooleanProperties MSTAGE_ENABLE_CLEANSING = new BooleanProperties("ms.enable.cleansing", Boolean.TRUE) {
    @Override
    public boolean isDeprecated() {
      return true;
    }
  };

  BooleanProperties MSTAGE_ENABLE_DYNAMIC_FULL_LOAD = new BooleanProperties("ms.enable.dynamic.full.load", Boolean.TRUE);
  BooleanProperties MSTAGE_ENABLE_SCHEMA_BASED_FILTERING = new BooleanProperties("ms.enable.schema.based.filtering", Boolean.TRUE);
  JsonArrayProperties MSTAGE_ENCRYPTION_FIELDS = new JsonArrayProperties("ms.encryption.fields") {
    @Override
    public boolean isValid(State state) {
      if (super.isValid(state) && !isBlank(state)) {
        // Encrypted fields cannot be nullable, required: isNullable = false
        if (!MSTAGE_OUTPUT_SCHEMA.isBlank(state)) {
          JsonArray encryptionFields = GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
          for (JsonElement field : encryptionFields) {
            if (!field.isJsonPrimitive() || field.getAsString().isEmpty() || SchemaUtils.isNullable(field.getAsString(),
                MSTAGE_OUTPUT_SCHEMA.get(state))) {
              return false;
            }
          }
        }
      }
      return super.isValid(state);
    }
  };
  StringProperties MSTAGE_EXTRACTOR_CLASS = new StringProperties("ms.extractor.class");
  StringProperties MSTAGE_EXTRACTOR_TARGET_FILE_NAME = new StringProperties("ms.extractor.target.file.name");
  StringProperties MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION = new StringProperties("ms.extractor.target.file.permission", "755");
  StringProperties MSTAGE_EXTRACT_PREPROCESSORS = new StringProperties("ms.extract.preprocessors");
  JsonObjectProperties MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS = new JsonObjectProperties("ms.extract.preprocessor.parameters");

  // default: 0, minimum: 0, maximum: -
  IntegerProperties MSTAGE_GRACE_PERIOD_DAYS = new IntegerProperties("ms.grace.period.days") {
    @Override
    public Long getMillis(State state) {
      return 24L * 3600L * 1000L * this.get(state);
    }
  };

  // ms.http.maxConnections has default value 50 and max value 500
  // 0 is interpreted as default
  IntegerProperties MSTAGE_HTTP_CONN_MAX =
      new IntegerProperties("ms.http.conn.max", 50, 500) {
        @Override
        protected Integer getValidNonblankWithDefault(State state) {
          int value = super.getValidNonblankWithDefault(state);
          return value == 0 ? getDefaultValue() : value;
        }
      };

  // ms.http.maxConnectionsPerRoute has default value 20 and max value 200
  // 0 is interpreted as default
  IntegerProperties MSTAGE_HTTP_CONN_PER_ROUTE_MAX =
      new IntegerProperties("ms.http.conn.per.route.max", 20, 200) {
        @Override
        protected Integer getValidNonblankWithDefault(State state) {
          int value = super.getValidNonblankWithDefault(state);
          return value == 0 ? getDefaultValue() : value;
        }
      };

  /**
   * see org.apache.http.impl.client.HttpClientBuilder#connTimeToLive
   */
  IntegerProperties MSTAGE_HTTP_CONN_TTL_SECONDS = new IntegerProperties("ms.http.conn.ttl.seconds", 10) {
    @Override
    public Long getMillis(State state) {
      return 1000L * this.get(state);
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
  BooleanProperties MSTAGE_METRICS_ENABLED = new BooleanProperties("ms.metrics.enabled", Boolean.FALSE);
  KafkaProperties MSTAGE_KAFKA_PROPERTIES = new KafkaProperties("ms.kafka");
  StringProperties MSTAGE_REPORTER_CLASS = new StringProperties("ms.reporter.class",
      "com.linkedin.cdi.factory.producer.KafkaEventReporter");


  // default: 500, minimum: 1, maximum: -
  LongProperties MSTAGE_NORMALIZER_BATCH_SIZE = new LongProperties("ms.normalizer.batch.size", 500L, Long.MAX_VALUE, 1L);

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
  // default: 50K, minimum: 1, maximum: -
  LongProperties MSTAGE_RANGE_GENERATOR_BATCH_SIZE = new LongProperties("ms.range.generator.batch.size", 50000L, Long.MAX_VALUE, 1L);
  //  set highest possible value to 10 Zs
  StringProperties MSTAGE_RANGE_GENERATOR_MAX_VALUE = new StringProperties("ms.range.generator.max.value", "zzzzzzzzzz");

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

  // default: 1000, minimum: 1, maximum: -
  IntegerProperties MSTAGE_S3_LIST_MAX_KEYS = new IntegerProperties("ms.s3.list.max.keys", 1000, Integer.MAX_VALUE, 1);

  JsonObjectProperties MSTAGE_SCHEMA_CLEANSING = new JsonObjectProperties("ms.schema.cleansing");
  SecondaryInputProperties MSTAGE_SECONDARY_INPUT = new SecondaryInputProperties("ms.secondary.input");
  StringProperties MSTAGE_SECRET_MANAGER_CLASS = new StringProperties("ms.secret.manager.class", "com.linkedin.cdi.util.GobblinSecretManager");
  JsonObjectProperties MSTAGE_SESSION_KEY_FIELD = new JsonObjectProperties("ms.session.key.field");
  JsonObjectProperties MSTAGE_AUX_KEYS = new JsonObjectProperties("ms.aux.keys");

  // default: 60 seconds, minimum: 0, maximum: -
  IntegerProperties MSTAGE_SFTP_CONN_TIMEOUT_MILLIS = new IntegerProperties("ms.sftp.conn.timeout.millis", 60000);

  StringProperties MSTAGE_SOURCE_DATA_CHARACTER_SET = new StringProperties("ms.source.data.character.set",
      StandardCharsets.UTF_8.toString());

  StringProperties MSTAGE_SOURCE_FILES_PATTERN = new StringProperties("ms.source.files.pattern", REGEXP_DEFAULT_PATTERN);
  JsonObjectProperties MSTAGE_SOURCE_S3_PARAMETERS = new JsonObjectProperties("ms.source.s3.parameters");
  StringProperties MSTAGE_SOURCE_SCHEMA_URN = new StringProperties("ms.source.schema.urn");
  StringProperties MSTAGE_SOURCE_URI = new StringProperties("ms.source.uri");

  SslProperties MSTAGE_SSL = new SslProperties("ms.ssl");
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

  // default: 600 second, minimum: 0 second, maximum: 24 hours
  LongProperties MSTAGE_WAIT_TIMEOUT_SECONDS = new LongProperties("ms.wait.timeout.seconds", 600L, 24 * 3600L, 0L) {
    @Override
    public Long getMillis(State state) {
      return 1000L * this.get(state);
    }
  };


  WatermarkProperties MSTAGE_WATERMARK = new WatermarkProperties("ms.watermark");
  JsonArrayProperties MSTAGE_WATERMARK_GROUPS = new JsonArrayProperties("ms.watermark.groups");

  // default: 0, minimum: 0, maximum: -
  LongProperties MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME = new LongProperties("ms.work.unit.scheduling.starttime");

  // default: 0, minimum: 0, maximum: -
  LongProperties MSTAGE_WORK_UNIT_MIN_RECORDS = new LongProperties("ms.work.unit.min.records");

  // default: 0, minimum: 0, maximum: -
  LongProperties MSTAGE_WORK_UNIT_MIN_UNITS = new LongProperties("ms.work.unit.min.units");

  // default: 0, minimum: 0, maximum: -
  IntegerProperties MSTAGE_WORK_UNIT_PACING_SECONDS = new IntegerProperties("ms.work.unit.pacing.seconds") {
    @Override
    public Long getMillis(State state) {
      return 1000L * this.get(state);
    }
  };

  // default: 100, minimum: -1, maximum: 1000, 0 = default value, -1 = max int
  IntegerProperties MSTAGE_WORK_UNIT_PARALLELISM_MAX = new IntegerProperties("ms.work.unit.parallelism.max", 500, 5000, -1) {
    @Override
    protected Integer getValidNonblankWithDefault(State state) {
      int value = super.getValidNonblankWithDefault(state);
      switch (value) {
        case 0:
          return getDefaultValue();
        case -1:
          return Integer.MAX_VALUE;
        default:
          return value;
      }
    }
  };

  BooleanProperties MSTAGE_WORK_UNIT_PARTIAL_PARTITION =
      new BooleanProperties("ms.work.unit.partial.partition", Boolean.TRUE);
  StringProperties MSTAGE_WORK_UNIT_PARTITION = new StringProperties("ms.work.unit.partition", "none");
  StringProperties CONVERTER_AVRO_DATE_FORMAT = new StringProperties("converter.avro.date.format");
  StringProperties CONVERTER_AVRO_TIME_FORMAT = new StringProperties("converter.avro.time.format");
  StringProperties CONVERTER_AVRO_TIMESTAMP_FORMAT = new StringProperties("converter.avro.timestamp.format");
  StringProperties CONVERTER_CLASSES = new StringProperties("converter.classes");
  StringProperties DATA_PUBLISHER_FINAL_DIR = new StringProperties("data.publisher.final.dir");
  StringProperties DATASET_URN = new StringProperties("dataset.urn");
  StringProperties ENCRYPT_KEY_LOC = new StringProperties("encrypt.key.loc");
  StringProperties EXTRACTOR_CLASSES = new StringProperties("extractor.class");
  // add a default value of FALSE to Gobblin configuration extract.is.full
  BooleanProperties EXTRACT_IS_FULL = new BooleanProperties("extract.is.full", Boolean.FALSE);
  StringProperties EXTRACT_NAMESPACE = new StringProperties("extract.namespace");

  // make extract.table.name required unless it is a file dump extractor
  StringProperties EXTRACT_TABLE_NAME = new StringProperties("extract.table.name") {
    @Override
    public boolean isValid(State state) {
      if (isBlank(state)) {
        if (!MSTAGE_EXTRACTOR_CLASS.get(state).equals(FileDumpExtractor.class.getName())) {
          return false;
        }
      }
      return super.isValid(state);
    }
  };

  StringProperties EXTRACT_TABLE_TYPE = new StringProperties("extract.table.type", "SNAPSHOT_ONLY") {
    @Override
    protected String getValidNonblankWithDefault(State state) {
      return super.getValidNonblankWithDefault(state).toUpperCase();
    }
  };

  StringProperties JOB_COMMIT_POLICY = new StringProperties("job.commit.policy");
  StringProperties JOB_DIR = new StringProperties("job.dir");
  StringProperties JOB_NAME = new StringProperties("job.name");
  StringProperties SOURCE_CLASS = new StringProperties("source.class");
  StringProperties SOURCE_CONN_HOST = new StringProperties("source.conn.host");
  StringProperties SOURCE_CONN_KNOWN_HOSTS = new StringProperties("source.conn.known.hosts");
  StringProperties SOURCE_CONN_USERNAME = new StringProperties("source.conn.username");
  StringProperties SOURCE_CONN_PASSWORD = new StringProperties("source.conn.password");
  StringProperties SOURCE_CONN_PORT = new StringProperties("source.conn.port");
  StringProperties SOURCE_CONN_PRIVATE_KEY = new StringProperties("source.conn.private.key");
  StringProperties SOURCE_CONN_USE_PROXY_URL = new StringProperties("source.conn.use.proxy.url");
  StringProperties SOURCE_CONN_USE_PROXY_PORT = new StringProperties("source.conn.use.proxy.port");
  StringProperties STATE_STORE_DIR = new StringProperties("state.store.dir");
  BooleanProperties STATE_STORE_ENABLED = new BooleanProperties("state.store.enabled", Boolean.TRUE);
  StringProperties STATE_STORE_TYPE = new StringProperties("state.store.type");
  IntegerProperties TASK_MAXRETRIES = new IntegerProperties("task.maxretries", 4);
  IntegerProperties TASKEXECUTOR_THREADPOOL_SIZE = new IntegerProperties("taskexecutor.threadpool.size", 10);

  List<MultistageProperties<?>> allProperties = Lists.newArrayList(
      MSTAGE_ABSTINENT_PERIOD_DAYS,
      MSTAGE_ACTIVATION_PROPERTY,
      MSTAGE_AUTHENTICATION,
      MSTAGE_BACKFILL,
      MSTAGE_CALL_INTERVAL_MILLIS,
      MSTAGE_CONNECTION_CLIENT_FACTORY,
      MSTAGE_CSV,
      MSTAGE_DATA_EXPLICIT_EOF,
      MSTAGE_DATA_DEFAULT_TYPE,
      MSTAGE_DATA_FIELD,
      MSTAGE_DERIVED_FIELDS,
      MSTAGE_ENABLE_CLEANSING,
      MSTAGE_ENABLE_DYNAMIC_FULL_LOAD,
      MSTAGE_ENABLE_SCHEMA_BASED_FILTERING,
      MSTAGE_ENCRYPTION_FIELDS,
      MSTAGE_EXTRACTOR_CLASS,
      MSTAGE_EXTRACTOR_TARGET_FILE_NAME,
      MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION,
      MSTAGE_EXTRACT_PREPROCESSORS,
      MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS,
      MSTAGE_GRACE_PERIOD_DAYS,
      MSTAGE_HTTP_CONN_MAX,
      MSTAGE_HTTP_CONN_PER_ROUTE_MAX,
      MSTAGE_HTTP_CONN_TTL_SECONDS,
      MSTAGE_HTTP_REQUEST_HEADERS,
      MSTAGE_HTTP_REQUEST_METHOD,
      MSTAGE_HTTP_RESPONSE_TYPE,
      MSTAGE_HTTP_STATUSES,
      MSTAGE_HTTP_STATUS_REASONS,
      MSTAGE_JDBC_SCHEMA_REFACTOR,
      MSTAGE_JDBC_STATEMENT,
      MSTAGE_KAFKA_PROPERTIES,
      MSTAGE_NORMALIZER_BATCH_SIZE,
      MSTAGE_OUTPUT_SCHEMA,
      MSTAGE_PAGINATION,
      MSTAGE_PARAMETERS,
      MSTAGE_PAYLOAD_PROPERTY,
      MSTAGE_RETENTION,
      MSTAGE_S3_LIST_MAX_KEYS,
      MSTAGE_SCHEMA_CLEANSING,
      MSTAGE_SECONDARY_INPUT,
      MSTAGE_SECRET_MANAGER_CLASS,
      MSTAGE_SESSION_KEY_FIELD,
      MSTAGE_AUX_KEYS,
      MSTAGE_SFTP_CONN_TIMEOUT_MILLIS,
      MSTAGE_SOURCE_DATA_CHARACTER_SET,
      MSTAGE_SOURCE_FILES_PATTERN,
      MSTAGE_SOURCE_S3_PARAMETERS,
      MSTAGE_SOURCE_SCHEMA_URN,
      MSTAGE_SOURCE_URI,
      MSTAGE_SSL,
      MSTAGE_TARGET_SCHEMA,
      MSTAGE_TARGET_SCHEMA_URN,
      MSTAGE_TOTAL_COUNT_FIELD,
      MSTAGE_VALIDATION_ATTRIBUTES,
      MSTAGE_WAIT_TIMEOUT_SECONDS,
      MSTAGE_WATERMARK,
      MSTAGE_WATERMARK_GROUPS,
      MSTAGE_WORK_UNIT_SCHEDULING_STARTTIME,
      MSTAGE_WORK_UNIT_MIN_RECORDS,
      MSTAGE_WORK_UNIT_MIN_UNITS,
      MSTAGE_WORK_UNIT_PACING_SECONDS,
      MSTAGE_WORK_UNIT_PARALLELISM_MAX,
      MSTAGE_WORK_UNIT_PARTIAL_PARTITION,
      MSTAGE_WORK_UNIT_PARTITION,
      CONVERTER_AVRO_DATE_FORMAT,
      CONVERTER_AVRO_TIME_FORMAT,
      CONVERTER_AVRO_TIMESTAMP_FORMAT,
      CONVERTER_CLASSES,
      DATA_PUBLISHER_FINAL_DIR,
      DATASET_URN,
      ENCRYPT_KEY_LOC,
      EXTRACTOR_CLASSES,
      EXTRACT_IS_FULL,
      EXTRACT_NAMESPACE,
      EXTRACT_TABLE_NAME,
      EXTRACT_TABLE_TYPE,
      JOB_COMMIT_POLICY,
      JOB_DIR,
      JOB_NAME,
      SOURCE_CLASS,
      SOURCE_CONN_HOST,
      SOURCE_CONN_KNOWN_HOSTS,
      SOURCE_CONN_PASSWORD,
      SOURCE_CONN_PORT,
      SOURCE_CONN_PRIVATE_KEY,
      SOURCE_CONN_USERNAME,
      SOURCE_CONN_USE_PROXY_URL,
      SOURCE_CONN_USE_PROXY_PORT,
      STATE_STORE_DIR,
      STATE_STORE_ENABLED,
      STATE_STORE_TYPE,
      TASK_MAXRETRIES,
      TASKEXECUTOR_THREADPOOL_SIZE
  );
  Map<String, MultistageProperties<?>> deprecatedProperties =
      new ImmutableMap.Builder<String, MultistageProperties<?>>()
          .put("dataset.name", EXTRACT_TABLE_NAME)
          .put("ms.csv.column.header", MSTAGE_CSV)
          .put("ms.csv.column.header.index", MSTAGE_CSV)
          .put("ms.csv.column.projection", MSTAGE_CSV)
          .put("ms.csv.default.field.type", MSTAGE_CSV)
          .put("ms.csv.escape.character", MSTAGE_CSV)
          .put("ms.csv.quote.character", MSTAGE_CSV)
          .put("ms.csv.separator", MSTAGE_CSV)
          .put("ms.csv.skip.lines", MSTAGE_CSV)
          .put("ms.converter.csv.max.failures", MSTAGE_CSV)
          .put("ms.converter.keep.null.strings", MSTAGE_CSV)
          .put("csv.max.failures", MSTAGE_CSV)
          .put("sftpConn.timeout", MSTAGE_SFTP_CONN_TIMEOUT_MILLIS)
          .build();
}
