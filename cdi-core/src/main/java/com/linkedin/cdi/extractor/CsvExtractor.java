// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.extractor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.linkedin.cdi.configuration.StaticConstants;
import com.linkedin.cdi.util.JsonUtils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.filter.CsvSchemaBasedFilter;
import com.linkedin.cdi.keys.CsvExtractorKeys;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.preprocessor.InputStreamProcessor;
import com.linkedin.cdi.preprocessor.StreamProcessor;
import com.linkedin.cdi.util.CsvUtils;
import com.linkedin.cdi.util.JsonIntermediateSchema;
import com.linkedin.cdi.util.SchemaBuilder;
import com.linkedin.cdi.util.SchemaUtils;
import com.linkedin.cdi.util.VariableUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.testng.Assert;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * CSV Extractor extracts CSV formatted data from an InputStream passed from a Source.
 *
 * The extractor accepts configurable preprocessors that transforms one InputStream to another
 * InputStream. Those preprocessors include Gunzip and GPG decryption steps
 *
 * @author chrli, esong
 */
@Slf4j
public class CsvExtractor extends MultistageExtractor<String, String[]> {
  private final static Long SCHEMA_INFER_MAX_SAMPLE_SIZE = 100L;
  @Getter
  private CsvExtractorKeys csvExtractorKeys = new CsvExtractorKeys();

  public CsvExtractor(WorkUnitState state, JobKeys jobKeys) {
    super(state, jobKeys);
    super.initialize(csvExtractorKeys);
    initialize(csvExtractorKeys);
  }

  @Override
  protected void initialize(ExtractorKeys keys) {
    csvExtractorKeys.logUsage(state);
    csvExtractorKeys.setColumnHeader(
        MultistageProperties.MSTAGE_CSV_COLUMN_HEADER.validateNonblank(state) ? MultistageProperties.MSTAGE_CSV_COLUMN_HEADER.getProp(
            state) : false);
    csvExtractorKeys.setRowsToSkip(MultistageProperties.MSTAGE_CSV_SKIP_LINES.getValidNonblankWithDefault(state));
    if (csvExtractorKeys.getColumnHeader()) {
      // only set the columnHeaderIndex if ms.csv.column.header is true
      csvExtractorKeys.setColumnHeaderIndex(
          MultistageProperties.MSTAGE_CSV_COLUMN_HEADER_INDEX.getValidNonblankWithDefault(state));
      // if no explicit number of lines to skip is set, skip all lines up to the header by default
      if (csvExtractorKeys.getRowsToSkip() == 0) {
        csvExtractorKeys.setRowsToSkip(csvExtractorKeys.getColumnHeaderIndex() + 1);
      } else if (csvExtractorKeys.getRowsToSkip() <= csvExtractorKeys.getColumnHeaderIndex()) {
        failWorkUnit(String.format("Header index out of bound: index is %d but the number of skipped lines is %d",
            csvExtractorKeys.getColumnHeaderIndex(), csvExtractorKeys.getRowsToSkip()));
      }
    }
    csvExtractorKeys.setSeparator(
        CsvUtils.unescape(MultistageProperties.MSTAGE_CSV_SEPARATOR.getValidNonblankWithDefault(state)));
    csvExtractorKeys.setQuoteCharacter(
        CsvUtils.unescape(MultistageProperties.MSTAGE_CSV_QUOTE_CHARACTER.getValidNonblankWithDefault(state)));
    csvExtractorKeys.setEscapeCharacter(
        CsvUtils.unescape(MultistageProperties.MSTAGE_CSV_ESCAPE_CHARACTER.getValidNonblankWithDefault(state)));
    csvExtractorKeys.setDefaultFieldType(
        MultistageProperties.MSTAGE_CSV_DEFAULT_FIELD_TYPE.getValidNonblankWithDefault(state).toString().toLowerCase());
    csvExtractorKeys.setSampleRows(new ArrayDeque<>());

    // check if user has defined the output schema
    if (jobKeys.hasOutputSchema()) {
      JsonArray outputSchema = jobKeys.getOutputSchema();
      csvExtractorKeys.setColumnProjection(expandColumnProjection(MultistageProperties.MSTAGE_CSV_COLUMN_PROJECTION
          .getValidNonblankWithDefault(state), outputSchema.size()));
      // initialize the column name to index map based on the schema when derived fields are present
      if (jobKeys.getDerivedFields().entrySet().size() > 0) {
        buildColumnToIndexMap(outputSchema);
      }
    }
    csvExtractorKeys.logDebugAll(state.getWorkunit());
  }

  /**
   * Utility function to do a double assignment
   * @param csvExtractorKeys the extractor key
   */
  @VisibleForTesting
  protected void setCsvExtractorKeys(CsvExtractorKeys csvExtractorKeys) {
    this.extractorKeys = csvExtractorKeys;
    this.csvExtractorKeys = csvExtractorKeys;
  }

  /**
   * This method rely on the parent class to get a JsonArray formatted schema, and pass it out as
   * a string. Typically we expect the downstream is a CsvToJsonConverter.
   *
   * @return schema that is structured as a JsonArray but formatted as a String
   */
  @Override
  public String getSchema() {
    log.debug("Retrieving schema definition");
    JsonArray schemaArray = super.getOrInferSchema();
    Assert.assertNotNull(schemaArray);
    if (jobKeys.getDerivedFields().size() > 0 && JsonUtils.get(StaticConstants.KEY_WORD_COLUMN_NAME,
        jobKeys.getDerivedFields().keySet().iterator().next(), StaticConstants.KEY_WORD_COLUMN_NAME, schemaArray) == JsonNull.INSTANCE) {
      schemaArray.addAll(addDerivedFieldsToAltSchema());
    }
    return schemaArray.toString();
  }

  /**
   * if pagination is not enabled, this method will iterate through the iterator and send records one
   * by one, each row formatted as a String[].
   *
   * if pagination is enabled, the method will try to get a new set of data from the Source after
   * the iterator is exhausted.
   *
   * @param reuse not used, just to match interface
   * @return a row of CSV data in String[] format
   */
  @Nullable
  @Override
  public String[] readRecord(String[] reuse) {
    super.readRecord(reuse);

    if (csvExtractorKeys.getCsvIterator() == null && !processInputStream(0)) {
      return null;
    }

    Iterator<String[]> readerIterator = csvExtractorKeys.getCsvIterator();
    if (csvExtractorKeys.getSampleRows().size() > 0) {
      csvExtractorKeys.incrProcessedCount();
      // update work unit status along the way, since we are using iterators
      workUnitStatus.setPageStart(csvExtractorKeys.getProcessedCount());
      workUnitStatus.setPageNumber(csvExtractorKeys.getCurrentPageNumber());
      String[] row = csvExtractorKeys.getSampleRows().pollFirst();
      return addDerivedFields(row);
    } else if (readerIterator.hasNext()) {
      csvExtractorKeys.incrProcessedCount();
      // update work unit status along the way, since we are using iterators
      workUnitStatus.setPageStart(csvExtractorKeys.getProcessedCount());
      workUnitStatus.setPageNumber(csvExtractorKeys.getCurrentPageNumber());
      // filtering is only required when schema is defined
      String[] row = readerIterator.next();
      CsvSchemaBasedFilter csvSchemaBasedFilter = (CsvSchemaBasedFilter) rowFilter;
      if (csvSchemaBasedFilter != null) {
        row = csvSchemaBasedFilter.filter(row);
        // when column projection is specified, the filter data should be the same size as the column projection
        if (csvExtractorKeys.getColumnProjection().size() > 0 && row.length != csvExtractorKeys.getColumnProjection()
            .size()) {
          failWorkUnit("Some indicies in column projection are out of bound");
        }
      }
      return addDerivedFields(row);
    } else {
      connection.closeStream();
      if (hasNextPage() && processInputStream(csvExtractorKeys.getProcessedCount())) {
        return readRecord(reuse);
      }
    }
    return null;
  }

  /**
   * This is the main method in this extractor, it extracts data from source and perform essential checks.
   *
   * @param starting the initial record count, indicating if it is the first of a series of requests
   * @return true if Successful
   */
  @Override
  protected boolean processInputStream(long starting) {
    if (!super.processInputStream(starting)) {
      return false;
    }

    // if Content-Type is provided, but not text/csv, the response can have
    // useful error information
    JsonObject expectedContentType = MultistageProperties.MSTAGE_HTTP_RESPONSE_TYPE.getValidNonblankWithDefault(state);
    HashSet<String> expectedContentTypeSet =
        new LinkedHashSet<>(Arrays.asList("text/plain", "text/csv", "application/gzip"));
    if (expectedContentType.has(CONTENT_TYPE_KEY) || expectedContentType.has(CONTENT_TYPE_KEY.toLowerCase())) {
      for (Map.Entry<String, JsonElement> entry: expectedContentType.entrySet()) {
        expectedContentTypeSet.add(entry.getValue().getAsString());
      }
    }

    if (!checkContentType(workUnitStatus, expectedContentTypeSet)) {
      return false;
    }

    if (workUnitStatus.getBuffer() != null) {
      try {
        InputStream input = workUnitStatus.getBuffer();
        for (StreamProcessor<?> transformer : extractorKeys.getPreprocessors()) {
          if (transformer instanceof InputStreamProcessor) {
            input = ((InputStreamProcessor) transformer).process(input);
          }
        }

        CSVParser parser = new CSVParserBuilder().withSeparator(csvExtractorKeys.getSeparator().charAt(0))
            .withQuoteChar(csvExtractorKeys.getQuoteCharacter().charAt(0))
            .withEscapeChar(csvExtractorKeys.getEscapeCharacter().charAt(0))
            .build();
        CSVReader reader = new CSVReaderBuilder(new InputStreamReader(input, Charset.forName(
            MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET.getValidNonblankWithDefault(state)))).withCSVParser(parser)
            .build();
        Iterator<String[]> readerIterator = reader.iterator();

        // header row can be in the front of informational rows or after them
        skipRowAndSaveHeader(readerIterator);

        // convert some sample data to json to infer the schema
        if (!jobKeys.hasOutputSchema() && starting == 0) {
          // initialize a reader without skipping lines since header might be used
          JsonArray inferredSchema = inferSchemaWithSample(readerIterator);
          extractorKeys.setInferredSchema(inferredSchema);

          // build the columnToIndexMap for derived fields based on the inferred schema
          if (jobKeys.getDerivedFields().entrySet().size() != 0) {
            buildColumnToIndexMap(inferredSchema);
          }
        }
        csvExtractorKeys.setCsvIterator(readerIterator);
      } catch (Exception e) {
        log.error("Error reading the input stream: {}", e.getMessage());
        return false;
      }
    }

    // return false to stop the job under these situations
    if (workUnitStatus.getBuffer() == null || csvExtractorKeys.getCsvIterator() == null) {
      return false;
    }
    csvExtractorKeys.incrCurrentPageNumber();

    csvExtractorKeys.logDebugAll(state.getWorkunit());
    workUnitStatus.logDebugAll();
    extractorKeys.logDebugAll(state.getWorkunit());

    return hasNext();
  }

  /**
   * Initialize row filter
   * @param schemaArray schema array
   */
  @Override
  protected void setRowFilter(JsonArray schemaArray) {
    if (rowFilter == null) {
      if (MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getValidNonblankWithDefault(state)) {
        rowFilter = new CsvSchemaBasedFilter(new JsonIntermediateSchema(schemaArray), csvExtractorKeys);
      }
    }
  }

  /**
   * Expand a column projection input string
   * @param columnProjection columns to project
   * @param numColumnsInPredefinedSchema number of columns
   * @return a set of column indices
   */
  private Set<Integer> expandColumnProjection(String columnProjection, int numColumnsInPredefinedSchema) {
    Set<Integer> expandedColumnProjection = new HashSet<>();
    if (columnProjection != null && columnProjection.length() > 0) {
      for (String val : columnProjection.split(",")) {
        if (val.matches("^(\\d+)-(\\d+)$")) {  // range
          int left = Integer.parseInt(val.split("-")[0]);
          int right = Integer.parseInt(val.split("-")[1]);
          if (left < 0 || right < 0 || left >= right) {
            failWorkUnit(String.format("Invalid range in column projection input %s", val));
            break;
          } else {
            for (int i = left; i <= right; i++) {
              expandedColumnProjection.add(i);
            }
          }
        } else if (val.matches("^\\d+$")) {  // single number
          int col = Integer.parseInt(val);
          if (col < 0) {
            failWorkUnit(String.format("Invalid index in column projection input %s", val));
            break;
          } else {
            expandedColumnProjection.add(col);
          }
        } else {  // unknown patterns
          failWorkUnit(String.format("Invalid value in column projection input %s", val));
          break;
        }
      }

      if (expandedColumnProjection.size() != numColumnsInPredefinedSchema) {
        failWorkUnit("The number of columns in column projection does not match the size of the predefined schema");
      }
    }
    return expandedColumnProjection;
  }

  /**
   * Helper function that builds the column name to index map
   * @param schema the Avro-flavor schema
   */
  private void buildColumnToIndexMap(JsonArray schema) {
    Map<String, Integer> columnToIndexMap = new HashMap<>();
    int index = 0;
    for (JsonElement column : schema) {
      String columnName = column.getAsJsonObject().get("columnName").getAsString();
      columnToIndexMap.put(columnName, index++);
    }
    csvExtractorKeys.setColumnToIndexMap(columnToIndexMap);
  }

  /**
   * Process the derived field source to get intermediate value
   * @param row current row being processed
   * @param derivedFieldDef map {type: type1, source: source1, format: format1}
   * @return String value of the derived field
   */
  private String processDerivedFieldSource(String[] row, String name, Map<String, String> derivedFieldDef) {
    String source = derivedFieldDef.getOrDefault("source", StringUtils.EMPTY);
    String inputValue = derivedFieldDef.getOrDefault("value", StringUtils.EMPTY);
    boolean isInputValueFromSource = false;

    if (csvExtractorKeys.getColumnToIndexMap().containsKey(source)) {
      int sourceIndex = csvExtractorKeys.getColumnToIndexMap().get(source);
      inputValue = row[sourceIndex];
      isInputValueFromSource = true;
    }

    return generateDerivedFieldValue(name, derivedFieldDef, inputValue, isInputValueFromSource);
  }

  /**
   * calculate and add derived fields,
   * derivedFields map in this in structure {name1 : {type: type1, source: source1, format: format1}}
   * @param row original record
   * @return modified record
   */
  private String[] addDerivedFields(String[] row) {
    Set<Map.Entry<String, Map<String, String>>> derivedFields = jobKeys.getDerivedFields().entrySet();
    int numDerivedFields = derivedFields.size();
    if (numDerivedFields == 0) {
      return row;
    }
    // allocate a larger array to accommodate the derived fields
    int originalLength = row.length;
    row = Arrays.copyOf(row, originalLength + numDerivedFields);

    int index = originalLength;
    for (Map.Entry<String, Map<String, String>> derivedField : derivedFields) {
      String name = derivedField.getKey();
      Map<String, String> derivedFieldDef = derivedField.getValue();
      String strValue = processDerivedFieldSource(row, name, derivedFieldDef);
      String type = derivedFieldDef.get("type");
      if (SUPPORTED_DERIVED_FIELD_TYPES.contains(type)) {
        row[index] = strValue;
      } else {
        failWorkUnit("Unsupported type for derived fields: " + type);
      }
      index++;  // increment index so the next derived field is written to a new column
    }
    return row;
  }

  /**
   * Save the header row if present, and skip rows
   * @param readerIterator iterator of input stream
   */
  private void skipRowAndSaveHeader(Iterator<String[]> readerIterator) {
    int linesRead = 0;
    while (readerIterator.hasNext() && linesRead < csvExtractorKeys.getRowsToSkip()) {
      String[] line = getNextLineWithCleansing(readerIterator);
      // save the column header
      if (linesRead == csvExtractorKeys.getColumnHeaderIndex() && csvExtractorKeys.getColumnHeader()) {
        csvExtractorKeys.setHeaderRow(line);
        // check if header has all columns in schema
        if (jobKeys.hasOutputSchema()) {
          List<String> schemaColumns =
              new ArrayList<>(new JsonIntermediateSchema(jobKeys.getOutputSchema()).getColumns().keySet());
          List<String> headerRow = Arrays.asList(csvExtractorKeys.getHeaderRow());
          csvExtractorKeys.setIsValidOutputSchema(SchemaUtils.isValidOutputSchema(schemaColumns, headerRow));
        }
      }
      linesRead++;
    }
  }

  /**
   * Perform limited cleansing so that data can be processed by converters
   *
   * @param input the input data to be cleansed
   */
  private void limitedCleanse(String[] input) {
    for (int i = 0; i < input.length; i++) {
      input[i] = input[i].replaceAll("(\\s|\\$)", "_");
    }
  }

  /**
   * Read next row and cleanse the data if enabled
   * @param readerIterator iterator of input stream
   * @return next line
   */
  private String[] getNextLineWithCleansing(Iterator<String[]> readerIterator) {
    String[] line = readerIterator.next();
    if (jobKeys.isEnableCleansing()) {
      limitedCleanse(line);
    }
    return line;
  }

  /**
   * Infer schema based on sample data. Rows read while preparing the sample is saved in a queue to be read again later.
   * @param readerIterator iterator of input stream
   * @return inferred schema
   */
  private JsonArray inferSchemaWithSample(Iterator<String[]> readerIterator) {
    String[] header = csvExtractorKeys.getHeaderRow();
    JsonArray sample = new JsonArray();
    int linesRead = 0;
    // read record until iterator is empty or enough lines have been read for the sample
    while (readerIterator.hasNext() && linesRead < SCHEMA_INFER_MAX_SAMPLE_SIZE) {
      String[] line = readerIterator.next();
      // save the new line to the end of queue
      csvExtractorKeys.getSampleRows().offerLast(line);
      // add the current row data to the sample json
      JsonObject row = new JsonObject();
      for (int i = 0; i < line.length; i++) {
        // do not use headers as keys if the header row and data have different lengths
        String key = header != null && header.length == line.length ? header[i] : "col" + i;
        addParsedCSVData(key, line[i], row);
      }
      sample.add(row);
      linesRead++;
    }
    return SchemaBuilder.fromJsonData(sample)
        .buildAltSchema(jobKeys.getDefaultFieldTypes(), jobKeys.isEnableCleansing(),
            jobKeys.getSchemaCleansingPattern(), jobKeys.getSchemaCleansingReplacement(),
            jobKeys.getSchemaCleansingNullable())
        .getAsJsonArray();
  }

  /**
   * Helper function for creating sample json data for schema inference
   * Type conversion is required as all data will be parsed as string otherwise
   * Users can specify the default type for all fields using ms.csv.default.field.type or
   * specify the default for a specific field using ms.default.data.type.
   * @param key name of the column
   * @param data original data from a column
   * @param row json form of the row
   */
  private void addParsedCSVData(String key, String data, JsonObject row) {
    String defaultFieldType = csvExtractorKeys.getDefaultFieldType();
    if (defaultFieldType.equals(KEY_WORD_STRING)) {
      row.addProperty(key, data);
    } else if (defaultFieldType.equals(KEY_WORD_INT) || Ints.tryParse(data) != null) {
      row.addProperty(key, Integer.valueOf(data));
    } else if (defaultFieldType.equals(KEY_WORD_LONG) || Longs.tryParse(data) != null) {
      row.addProperty(key, Long.valueOf(data));
    } else if (defaultFieldType.equals(KEY_WORD_DOUBLE) || Doubles.tryParse(data) != null) {
      row.addProperty(key, Double.valueOf(data));
    } else if (defaultFieldType.equals(KEY_WORD_BOOLEAN) || data.toLowerCase().matches("(true|false)")) {
      row.addProperty(key, Boolean.valueOf(data));
    } else if (defaultFieldType.equals(KEY_WORD_FLOAT) || Floats.tryParse(data) != null) {
      row.addProperty(key, Float.valueOf(data));
    } else {
      row.addProperty(key, data);
    }
  }

  /**
   * Helper function that indicates if there are any records left to read
   * @return true if there are more records and false otherwise
   */
  protected boolean hasNext() {
    return csvExtractorKeys.getCsvIterator().hasNext() || csvExtractorKeys.getSampleRows().size() > 0;
  }

  /**
   * If the iterator is null, then it must be the first request
   * @param starting the starting position of the request
   * @return true if the iterator is null, otherwise false
   */
  @Override
  protected boolean isFirst(long starting) {
    return csvExtractorKeys.getCsvIterator() == null;
  }
}
