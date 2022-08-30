// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class SchemaBuilderTest {
  @Test
  public void testReverseJsonSchema() {
    String originSchema = "{\"id\":{\"type\":\"string\"}}";
    SchemaBuilder builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildJsonSchema().toString(), originSchema);

    originSchema = "{\"id\":{\"type\":[\"string\",\"null\"]}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildJsonSchema().toString(), originSchema);

    originSchema = "{\"methods\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildJsonSchema().toString(), originSchema);
  }

  @Test
  public void testAltSchema() {
    String originSchema = "{\"id\":{\"type\":\"string\"}}";
    SchemaBuilder builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"id\",\"isNullable\":false,\"dataType\":{\"type\":\"string\"}}]");
    Assert.assertEquals(builder.buildAltSchema(new HashMap<>(), false, null, null, true).toString(),
        "[{\"columnName\":\"id\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]");

    originSchema = "{\"id\":{\"type\":[\"string\",\"null\"]}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"id\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]");

    originSchema = "{\"methods\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}";
    builder = SchemaBuilder.fromJsonSchema(originSchema);
    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"methods\",\"isNullable\":false,\"dataType\":{\"type\":\"array\",\"name\":\"methods\",\"items\":\"string\"}}]");
  }

  @Test
  public void testAltSchemaForRecord() {
    String avroSchema = "{\"type\":\"record\",\"name\":\"UploadClickConversionsResponse\",\"namespace\":\"com.linkedin.coderising\",\"doc\":\"Response for Ads\\nhttps://developers.google.com/google-ads/api/reference/rpc/v6/UploadClickConversionsResponse\",\"fields\":[{\"name\":\"partialFailureError\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"partialFailureErrorRecord\",\"fields\":[{\"name\":\"code\",\"type\":[\"null\",\"int\"],\"doc\":\"code of the faliure\",\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"message\",\"type\":[\"null\",\"string\"],\"doc\":\"message of the failure\",\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"details\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"detailRecord\",\"doc\":\"detail records\",\"fields\":[{\"name\":\"_type\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"errors\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"errorRecord\",\"fields\":[{\"name\":\"errorCode\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"errorCodeRecord\",\"fields\":[{\"name\":\"conversionUploadError\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"}]}],\"default\":null},{\"name\":\"message\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"trigger\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"triggerRecord\",\"fields\":[{\"name\":\"stringValue\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"}]}],\"default\":null},{\"name\":\"location\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"locationRecord\",\"fields\":[{\"name\":\"fieldPathElements\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"fieldPathElementRecord\",\"fields\":[{\"name\":\"fieldName\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"index\",\"type\":[\"null\",\"int\"],\"default\":null,\"compliance\":\"NONE\"}]}}],\"default\":null}]}],\"default\":null}]}}],\"default\":null}]}}],\"doc\":\"details of the failure\",\"default\":null}]}],\"doc\":\"partial failure error\",\"default\":null},{\"name\":\"results\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"resultRecord\",\"fields\":[{\"name\":\"gclid\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"conversionAction\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"},{\"name\":\"conversionDateTime\",\"type\":[\"null\",\"string\"],\"default\":null,\"compliance\":\"NONE\"}]}}],\"doc\":\"Results\",\"default\":null}],\"collection\":{\"name\":\"UploadClickConversionsResponse\"}}";
    JsonObject schema = new Gson().fromJson(avroSchema, JsonObject.class);
    SchemaBuilder builder = SchemaBuilder.fromAvroSchema(schema);

    Assert.assertEquals(builder.buildAltSchema().toString(),
        "[{\"columnName\":\"partialFailureError\",\"dataType\":{\"type\":\"record\",\"name\":\"partialFailureError\",\"values\":[{\"columnName\":\"code\",\"isNullable\":true,\"dataType\":{\"type\":\"int\"}},{\"columnName\":\"message\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"details\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"details\",\"items\":{\"columnName\":\"arrayItem\",\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"_type\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"errors\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"errors\",\"items\":{\"columnName\":\"arrayItem\",\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"errorCode\",\"dataType\":{\"type\":\"record\",\"name\":\"errorCode\",\"values\":[{\"columnName\":\"conversionUploadError\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}},{\"columnName\":\"message\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"trigger\",\"dataType\":{\"type\":\"record\",\"name\":\"trigger\",\"values\":[{\"columnName\":\"stringValue\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}},{\"columnName\":\"location\",\"dataType\":{\"type\":\"record\",\"name\":\"location\",\"values\":[{\"columnName\":\"fieldPathElements\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"fieldPathElements\",\"items\":{\"columnName\":\"arrayItem\",\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"fieldName\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"index\",\"isNullable\":true,\"dataType\":{\"type\":\"int\"}}]}}}}]}}]}}}}]}}}}]}},{\"columnName\":\"results\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"results\",\"items\":{\"columnName\":\"arrayItem\",\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"gclid\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"conversionAction\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"conversionDateTime\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}}}}]");

    Assert.assertEquals(builder.buildAltSchema(true).toString(),
        "[{\"columnName\":\"partialFailureError\",\"isNullable\":true,\"dataType\":{\"type\":\"record\",\"name\":\"partialFailureError\",\"values\":[{\"columnName\":\"code\",\"isNullable\":true,\"dataType\":{\"type\":\"int\"}},{\"columnName\":\"message\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"details\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"details\",\"items\":{\"columnName\":\"arrayItem\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"_type\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"errors\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"errors\",\"items\":{\"columnName\":\"arrayItem\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"errorCode\",\"isNullable\":true,\"dataType\":{\"type\":\"record\",\"name\":\"errorCode\",\"values\":[{\"columnName\":\"conversionUploadError\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}},{\"columnName\":\"message\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"trigger\",\"isNullable\":true,\"dataType\":{\"type\":\"record\",\"name\":\"trigger\",\"values\":[{\"columnName\":\"stringValue\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}},{\"columnName\":\"location\",\"isNullable\":true,\"dataType\":{\"type\":\"record\",\"name\":\"location\",\"values\":[{\"columnName\":\"fieldPathElements\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"fieldPathElements\",\"items\":{\"columnName\":\"arrayItem\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"fieldName\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"index\",\"isNullable\":true,\"dataType\":{\"type\":\"int\"}}]}}}}]}}]}}}}]}}}}]}},{\"columnName\":\"results\",\"isNullable\":true,\"dataType\":{\"type\":\"array\",\"name\":\"results\",\"items\":{\"columnName\":\"arrayItem\",\"isNullable\":false,\"dataType\":{\"type\":\"record\",\"name\":\"arrayItem\",\"values\":[{\"columnName\":\"gclid\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"conversionAction\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"conversionDateTime\",\"isNullable\":true,\"dataType\":{\"type\":\"string\"}}]}}}}]");

  }
}
