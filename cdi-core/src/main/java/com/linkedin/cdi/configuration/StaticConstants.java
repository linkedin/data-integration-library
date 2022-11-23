// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.Gson;


public interface StaticConstants {
  String KEY_WORD_ACTIVATION = "activation";
  String KEY_WORD_ARRAY = "array";
  String KEY_WORD_ARRAY_ITEM = "arrayItem";
  String KEY_WORD_AUTHENTICATION = "authentication";
  String KEY_WORD_BOOLEAN = "boolean";
  String KEY_WORD_CATEGORY = "category";
  String KEY_WORD_COLUMN_NAME = "columnName";
  String KEY_WORD_COMMA = ",";
  String KEY_WORD_CUSTOM = "custom";
  String KEY_WORD_DATA_IS_NULLABLE = "isNullable";
  String KEY_WORD_DATA_TYPE = "dataType";
  String KEY_WORD_DATA_TYPE_TYPE = "dataType.type";
  String KEY_WORD_ENCRYPTION = "encryption";
  String KEY_WORD_EOF = "EOF";
  String KEY_WORD_EPOC = "epoc";
  String KEY_WORD_FIELDS = "fields";
  String KEY_WORD_FORMAT = "format";
  String KEY_WORD_FORMULA = "formula";
  String KEY_WORD_FROM = "from";
  String KEY_WORD_HEADER = "header";
  String KEY_WORD_HTTP_OK = "ok";
  String KEY_WORD_INTEGER = "integer";
  String KEY_WORD_ISO = "ISO";
  String KEY_WORD_IS_NULLABLE = "isNullable";
  String KEY_WORD_ITEMS = "items";
  String KEY_WORD_MAP = "map";
  String KEY_WORD_NAME = "name";
  String KEY_WORD_HTTP_NOTOK = "notok";
  String KEY_WORD_METHOD = "method";
  String KEY_WORD_NULL = "null";
  String KEY_WORD_NULLABLE = "nullable";
  String KEY_WORD_NUMBER = "number";
  String KEY_WORD_OBJECT = "object";
  String KEY_WORD_PATH = "path";
  String KEY_WORD_PAYLOAD = "payload";
  String KEY_WORD_PRIMITIVE = "primitive";
  String KEY_WORD_PROPERTIES = "properties";
  String KEY_WORD_RANGE = "range";
  String KEY_WORD_RECORD = "record";
  String KEY_WORD_REGEXP = "regexp";
  String KEY_WORD_RETRY = "retry";
  String KEY_WORD_RETRY_COUNT = "retryCount";
  String KEY_WORD_RETRY_DELAY_IN_SEC = "delayInSec";
  String KEY_WORD_ROOT = "root";
  String KEY_WORD_SNAPSHOT_ONLY = "SNAPSHOT_ONLY";
  String KEY_WORD_SOURCE = "source";
  String KEY_WORD_SOURCE_TYPE = "source.type";
  String KEY_WORD_STRING = "string";
  String KEY_WORD_SYMBOLS = "symbols";
  String KEY_WORD_TIMESTAMP = "timestamp";
  String KEY_WORD_TIMEZONE = "timezone";
  String KEY_WORD_TO = "to";
  String KEY_WORD_TOKEN = "token";
  String KEY_WORD_TYPE = "type";
  String KEY_WORD_UNITS = "units";
  String KEY_WORD_UNKNOWN = "unknown";
  String KEY_WORD_VALIDATION = "validation";
  String KEY_WORD_VALUES = "values";
  String KEY_WORD_THRESHOLD = "threshold";
  String KEY_WORD_CRITERIA = "criteria";
  String KEY_WORD_FAIL = "fail";
  String KEY_WORD_SUCCESS = "success";
  String KEY_WORD_ERROR_COLUMN = "errorColumn";
  String KEY_WORD_INT = "int";
  String KEY_WORD_LONG = "long";
  String KEY_WORD_DOUBLE = "double";
  String KEY_WORD_FLOAT = "float";
  String KEY_WORD_JSON = "json";
  String KEY_WORD_CSV = "csv";
  String KEY_WORD_AVRO = "avro";

  String TZ_UTC = "UTC";
  String TZ_LOS_ANGELES = "America/Los_Angeles";

  String PROPERTY_DELIMINATOR = ".";
  String REGEXP_DEFAULT_PATTERN = ".*";
  String REGEXP_TIME_DURATION_PATTERN = "P\\d+D(T\\d+H)?(\\d+M)?(\\..*)?";
  String REGEXP_DAY_ONLY_DURATION_PATTERN = "P\\d+D(\\..*)?";
  String REGEXP_HOUR_ONLY_DURATION_PATTERN = "P\\d+D(T\\d+H)(\\..*)?";

  String DOC_BASE_URL = "https://github.com/linkedin/data-integration-library/blob/master/docs";
  String CLEANSE_NO_RANGE_WORK_UNIT = "cleanseNoRangeWorkUnit";

  String EXCEPTION_WORK_UNIT_MINIMUM = "Job requires a minimum of %s work unit(s) to proceed because ms.work.unit.min.units = %s.";
  String EXCEPTION_RECORD_MINIMUM = "Work unit requires a minimum of %s record(s) to succeed because ms.work.unit.min.records = %s.";
  String EXCEPTION_INCORRECT_CONFIGURATION = "Property %s has incorrect configuration: %s, see: %s";
  String EXCEPTION_DEPRECATED_CONFIGURATION = "Property %s has been deprecated, and the replacement is: %s, see: %s";

  String ERROR_READING_SECONDARY_INPUT =  "Error reading %s secondary input for work unit %s";

  String MSG_ROWS_PROCESSED = "Processed %s records, work unit: %s";
  String MSG_WORK_UNIT_ALWAYS = "There should be a work unit.";
  String MSG_LOW_WATER_MARK_ALWAYS = "There should be a low watermark.";
  String MSG_WORK_UNIT_INFO = "Generating Work Unit: %s, watermark: %s";
  Gson GSON = new Gson();
}
