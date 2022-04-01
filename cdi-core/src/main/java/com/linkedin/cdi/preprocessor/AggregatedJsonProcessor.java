// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.preprocessor;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.cdi.configuration.PropertyCollection;
import com.linkedin.cdi.util.JsonUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * Preprocessor to inflate aggregated JSON so that extractor can process
 *
 */
public class AggregatedJsonProcessor extends InputStreamProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedJsonProcessor.class);

  /**
   * @param params See {@link PropertyCollection}
   */
  public AggregatedJsonProcessor(JsonObject params) {
    super(params);
  }

  @Override
  public InputStream process(InputStream inputStream) throws IOException {
    Preconditions.checkArgument(parameters.has("header"));
    Preconditions.checkArgument(parameters.has("data"));


    String unwrapPath = parameters.has("unwrap")
        ? parameters.get("unwrap").getAsString() : StringUtils.EMPTY;
    String headerPath = parameters.get("header").getAsString();
    String dataPath = parameters.get("data").getAsString();
    String[] otherFields = parameters.has("fields")
        ? parameters.get("fields").getAsString().split(KEY_WORD_COMMA) : new String[0];

    Path path = Files.createTempFile(null, null);
    File file = path.toFile();
    file.deleteOnExit();
    file.setReadable(true, true);
    if(inputStream != null) {
      JsonObject processed = new JsonObject();
      JsonElement input = new JsonParser().parse(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      if(!unwrapPath.isEmpty()) {
        input = JsonUtils.get(Arrays.stream(unwrapPath.split("\\.")).iterator(), input);
      }

      if(input != JsonNull.INSTANCE && input.isJsonObject()) {
        JsonElement header = JsonUtils.get(input.getAsJsonObject(), headerPath);
        JsonElement data = JsonUtils.get(input.getAsJsonObject(), dataPath);

        if(header != JsonNull.INSTANCE
            && data != JsonNull.INSTANCE
            && header.isJsonArray()
            && data.isJsonArray()) {
          JsonArray rows = new JsonArray();
          int n = header.getAsJsonArray().size();
          int recCount = 0;
          for(JsonElement row: data.getAsJsonArray()) {
            JsonObject record = new JsonObject();
            for(int i = 0; i < n; i++) {
              record.add(header.getAsJsonArray().get(i).getAsString(), row.getAsJsonArray().get(i));
            }
            recCount ++;
            rows.add(record);
          }
          processed.addProperty("count", recCount);
          processed.add("results", rows);

          for(String jsonPath: otherFields) {
            String[] segments = jsonPath.split("\\.");
            if(segments.length > 0) {
              String name = segments[segments.length - 1];
              JsonElement field = JsonUtils.get(input.getAsJsonObject(), jsonPath);
              if(field != JsonNull.INSTANCE) {
                processed.add(name, field);
              }
            }
          }

        }
      } else if(input.isJsonArray()) {
        // TODO
      }
      OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(path.toFile()), StandardCharsets.UTF_8);
      writer.write(processed.toString());
      writer.flush();
      writer.close();
    }
    return Files.newInputStream(path);
  }

  /**
   * @param fileName
   * @return transformed file name
   */
  @Override
  public String convertFileName(String fileName) {
    return fileName;
  }
}
