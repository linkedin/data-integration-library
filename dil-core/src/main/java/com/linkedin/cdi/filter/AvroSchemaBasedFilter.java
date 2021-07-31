// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.common.base.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.cdi.keys.AvroExtractorKeys;
import com.linkedin.cdi.util.AvroSchemaUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import org.apache.gobblin.util.AvroUtils;


@Slf4j
public class AvroSchemaBasedFilter extends MultistageSchemaBasedFilter<GenericRecord> {
  private AvroExtractorKeys avroExtractorKeys;
  private WorkUnitState state;

  public AvroSchemaBasedFilter(JsonIntermediateSchema schema, AvroExtractorKeys avroExtractorKeys,
      WorkUnitState state) {
    super(schema);
    this.avroExtractorKeys = avroExtractorKeys;
    this.state = state;
  }

  @SneakyThrows
  @Override
  public GenericRecord filter(GenericRecord input) {
    Schema outputSchema = AvroSchemaUtils.fromJsonSchema(schema.toJson(), state);
    GenericRecord filteredRow = new GenericData.Record(outputSchema);
    if (avroExtractorKeys.getIsValidOutputSchema()) {
      log.warn("Some columns from the schema are not present at source, padding with null value.");
    }
    for (String fieldName : AvroSchemaUtils.getSchemaFieldNames(outputSchema)) {
      Optional<Object> fieldValue = AvroUtils.getFieldValue(input, fieldName);
      filteredRow.put(fieldName, fieldValue.isPresent() ? fieldValue.get() : null);
    }
    return filteredRow;
  }
}
