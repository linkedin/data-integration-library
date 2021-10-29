// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.filter;

import com.google.common.base.Optional;
import com.linkedin.cdi.keys.AvroExtractorKeys;
import com.linkedin.cdi.util.AvroSchemaUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.util.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchemaBasedFilter extends MultistageSchemaBasedFilter<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaBasedFilter.class);
  private AvroExtractorKeys avroExtractorKeys;
  private WorkUnitState state;

  public AvroSchemaBasedFilter(JsonIntermediateSchema schema, AvroExtractorKeys avroExtractorKeys,
      WorkUnitState state) {
    super(schema);
    this.avroExtractorKeys = avroExtractorKeys;
    this.state = state;
  }

  @Override
  public GenericRecord filter(GenericRecord input) {
    Schema outputSchema = AvroSchemaUtils.fromJsonSchema(schema.toJson(), state);
    GenericRecord filteredRow = new GenericData.Record(outputSchema);
    for (String fieldName : AvroSchemaUtils.getSchemaFieldNames(outputSchema)) {
      Optional<Object> fieldValue = AvroUtils.getFieldValue(input, fieldName);
      filteredRow.put(fieldName, fieldValue.isPresent() ? fieldValue.get() : null);
    }
    return filteredRow;
  }
}
