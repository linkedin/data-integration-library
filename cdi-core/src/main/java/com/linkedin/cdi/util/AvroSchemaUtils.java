// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;
import org.apache.gobblin.converter.json.JsonSchema;


public interface AvroSchemaUtils {
  /**
   * Utility method to convert JsonArray schema to avro schema
   * @param schema of JsonArray type
   * @return avro schema
   * @throws UnsupportedDateTypeException unsupported type
   */
  static Schema fromJsonSchema(JsonArray schema, WorkUnitState state) {
    JsonSchema jsonSchema = new JsonSchema(schema);
    jsonSchema.setColumnName(state.getExtract().getTable());
    try {
      JsonElementConversionFactory.RecordConverter recordConverter =
          new JsonElementConversionFactory.RecordConverter(jsonSchema, state, state.getExtract().getNamespace());
      return recordConverter.schema();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Utility method to extract field names from an avro schema
   * @param schema avro schema
   * @return List of field names
   */
  static List<String> getSchemaFieldNames(Schema schema) {
    return schema.getFields().stream().map(Schema.Field::name).collect(
        Collectors.toList());
  }

  /**
   * Make a deep copy of a schema field
   * @param field schema field
   * @return copy of schema field
   */
  static Schema.Field deepCopySchemaField(Schema.Field field) {
    Schema.Field f = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
    field.getObjectProps().forEach(
        (propName, propValue) -> {
          if (propValue instanceof String) f.addProp(propName, (String) propValue);
        }
    );
    return f;
  }

  /**
   * Utility method to create record
   * @param state work unit state to get namespace info
   * @return a record with EOF
   */
  static GenericRecord createEOF(WorkUnitState state) {
    JsonArray eofSchema = new Gson()
        .fromJson("[{\"columnName\":\"EOF\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"string\"}}]",
            JsonArray.class);
    Schema schema = fromJsonSchema(eofSchema, state);
    assert (schema != null);
    GenericRecord eofRecord = new GenericData.Record(schema);
    eofRecord.put("EOF", "EOF");
    return eofRecord;
  }

  /**
   * Makes a deep copy of a value given its schema.
   * @param schema the schema of the value to deep copy.
   * @param value the value to deep copy.
   * @return a deep copy of the given value.
   */
  static <T> T deepCopy(Schema schema, T value) {
    return GenericData.get().deepCopy(schema, value);
  }
}
