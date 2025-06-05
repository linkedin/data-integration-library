// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * Recursively defined a Json Intermediate schema
 *
 * JsonIntermediateSchema := Map&lt;columnName, JisColumn&gt;
 *
 * JisColumn :=  (columnName, nullability, JisDataType)
 *
 * JisDataType := RecordType | ArrayType | EnumType | UnionType
 *
 * RecordType := (JsonElementType, JsonIntermediateSchema)
 *
 * ArrayType := (JsonElementType, JisDataType)
 *
 * EnumType := (JsonElementType, symbolsArray)
 *
 * UnionType := (JsonElementType, List&lt;JisDataType&gt;)
 *
 */


public class JsonIntermediateSchema {
  public static final String ROOT_RECORD_COLUMN_NAME = "root";
  public static final String CHILD_RECORD_COLUMN_NAME = "child";

  // LinkedHashMap maintains insertion order, so the key set will be in the same order as the output schema
  Map<String, JisColumn> columns = new LinkedHashMap<>();
  String schemaName;

  public Map<String, JisColumn> getColumns() {
    return columns;
  }

  public void setColumns(Map<String, JisColumn> columns) {
    this.columns = columns;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  // a JIS schema contains JIS columns
  public class JisColumn {
    String columnName;
    Boolean isNullable;
    JisDataType dataType;

    public String getColumnName() {
      return columnName;
    }

    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }

    public Boolean getIsNullable() {
      return isNullable;
    }

    public void setIsNullable(Boolean nullable) {
      isNullable = nullable;
    }

    public JisDataType getDataType() {
      return dataType;
    }

    public void setDataType(JisDataType dataType) {
      this.dataType = dataType;
    }

    // define a simple column
    JisColumn(String name, Boolean isNullable, String type) {
      this.setColumnName(name);
      this.setIsNullable(isNullable);
      this.setDataType(new JisDataType(type));
    }

    // define a complex column
    JisColumn(JsonObject columnDefinition) {
      try {
        if (columnDefinition.has(KEY_WORD_COLUMN_NAME)) {
          this.setColumnName(columnDefinition.get(KEY_WORD_COLUMN_NAME).getAsString());
        } else if (columnDefinition.has(KEY_WORD_NAME)) {
          this.setColumnName(columnDefinition.get(KEY_WORD_COLUMN_NAME).getAsString());
        } else {
          this.setColumnName(KEY_WORD_UNKNOWN);
        }

        // set default as NULLABLE if column definition did not specify
        if (columnDefinition.has(KEY_WORD_DATA_IS_NULLABLE)) {
          this.setIsNullable(Boolean.valueOf(columnDefinition.get(KEY_WORD_DATA_IS_NULLABLE).getAsString()));
        } else {
          this.setIsNullable(Boolean.TRUE);
        }

        this.setDataType(new JisDataType(columnDefinition.get(KEY_WORD_DATA_TYPE).getAsJsonObject()));
      } catch (Exception e) {
        throw new RuntimeException("Incorrect column definition in JSON: " + columnDefinition.toString(), e);
      }
    }

    /**
     * Convert the column object to Json Object
     * @return a Json Object presentation of the column
     */
    public JsonObject toJson() {
      JsonObject column = new JsonObject();
      column.addProperty(KEY_WORD_COLUMN_NAME, this.getColumnName());
      column.addProperty(KEY_WORD_DATA_IS_NULLABLE, this.isNullable ? "true" : "false");
      column.add(KEY_WORD_DATA_TYPE, this.getDataType().toJson());
      return column;
    }
  }

  // a JIS Column has a JIS Data Type
  public class JisDataType {
    JsonElementTypes type;

    // data type name is optional
    String name;

    // values have the array of field definitions when the type is record
    JsonIntermediateSchema childRecord;

    // items have the item definition
    JisDataType itemType;

    // unions have item types
    List<JisDataType> itemTypes = Lists.newArrayList();

    JsonArray symbols;

    public JsonElementTypes getType() {
      return type;
    }

    public void setType(JsonElementTypes type) {
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public JsonIntermediateSchema getChildRecord() {
      return childRecord;
    }

    public void setChildRecord(JsonIntermediateSchema childRecord) {
      this.childRecord = childRecord;
    }

    public JisDataType getItemType() {
      return itemType;
    }

    public void setItemType(JisDataType itemType) {
      this.itemType = itemType;
    }

    public List<JisDataType> getItemTypes() {
      return itemTypes;
    }

    public void setItemTypes(List<JisDataType> itemTypes) {
      this.itemTypes = itemTypes;
    }

    public JsonArray getSymbols() {
      return symbols;
    }

    public void setSymbols(JsonArray symbols) {
      this.symbols = symbols;
    }

    // this defines primitive data type
    JisDataType(String type) {
      this.setType(JsonElementTypes.forType(type));
    }

    JisDataType(JsonObject dataTypeDefinition) {
      this.setType(JsonElementTypes.forType(dataTypeDefinition.get(KEY_WORD_TYPE).getAsString()));
      if (dataTypeDefinition.has(KEY_WORD_NAME)) {
        this.setName(dataTypeDefinition.get(KEY_WORD_NAME).getAsString());
      }
      switch (type) {
        case RECORD:
          // a record field is will have child schema
          this.setChildRecord(new JsonIntermediateSchema(CHILD_RECORD_COLUMN_NAME,
              dataTypeDefinition.get(KEY_WORD_VALUES).getAsJsonArray()));
          break;
        case ARRAY:
          // an array field will have a item type definition, which can be primitive or complex
          JsonElement itemDefinition = dataTypeDefinition.get(KEY_WORD_ITEMS);

          if (itemDefinition.isJsonPrimitive()) {
            this.setItemType(new JisDataType(itemDefinition.getAsString()));
          } else {
            // if not primitive, the item type is complex, and it has to be defined in a JsonObject
            this.setItemType(new JisDataType(itemDefinition.getAsJsonObject().get(KEY_WORD_DATA_TYPE).getAsJsonObject()));
          }
          break;
        case ENUM:
          // an Enum has a list of symbols
          this.setSymbols(dataTypeDefinition.get(KEY_WORD_SYMBOLS).getAsJsonArray());
          break;
        case MAP:
          JsonElement valuesElement = dataTypeDefinition.get(KEY_WORD_VALUES);
          if(valuesElement.isJsonPrimitive()) {
            this.setItemType(new JisDataType(valuesElement.getAsString()));
          } else {
            this.setItemType(new JisDataType(valuesElement.getAsJsonObject().get(KEY_WORD_DATA_TYPE).getAsJsonObject()));
          }
          break;
        case UNION:
          // a Union has 2 or more different types
          // TODO
          break;
        default:
          break;
      }
    }

    /** Convert the data type object to Json Object
     * @return a Json Object presentation of the data type
     */
    public JsonObject toJson() {
      JsonObject dataType = new JsonObject();
      dataType.addProperty(KEY_WORD_TYPE, this.getType().toString());
      dataType.addProperty(KEY_WORD_NAME, this.getName());
      switch (type) {
        case RECORD:
          dataType.add(KEY_WORD_VALUES, childRecord.toJson());
          break;
        case ARRAY:
          JsonObject itemsObject = new JsonObject();
          itemsObject.addProperty(KEY_WORD_NAME, this.getName());
          itemsObject.add(KEY_WORD_DATA_TYPE, itemType.toJson());
          dataType.add(KEY_WORD_ITEMS, itemsObject);
          break;
        default:
          break;
      }
      return dataType;
    }

    public boolean isPrimitive() {
      return JsonElementTypes.isPrimitive(type);
    }
  }

  /**
   * A Json Intermediate schema starts with a root column
   *
   * @param recordSchema the intermediate schema definition
   */
  public JsonIntermediateSchema(JsonArray recordSchema) {
    this.setSchemaName(ROOT_RECORD_COLUMN_NAME);
    addColumns(recordSchema);
  }

  /**
   * A Json Intermediate schema record can be a nested field
   *
   * @param fieldName the field name of the nested record
   * @param recordSchema the intermediate schema definition
   */
  public JsonIntermediateSchema(String fieldName, JsonArray recordSchema) {
    this.setSchemaName(fieldName);
    addColumns(recordSchema);
  }

  /**
   * add columns of a record
   * @param recordSchema the schema of the record
   */
  private void addColumns(JsonArray recordSchema) {
    for (JsonElement column: recordSchema) {
      Preconditions.checkArgument(column != null && column.isJsonObject());
      JisColumn col = new JisColumn(column.getAsJsonObject());
      columns.put(col.getColumnName(), col);
    }
  }

  /**
   * Convert the schema object to Json Array
   * @return a Json Array presentation of the schema
   */
  public JsonArray toJson() {
    JsonArray schema = new JsonArray();
    for (Map.Entry<String, JisColumn> entry: columns.entrySet()) {
      schema.add(entry.getValue().toJson());
    }
    return schema;
  }
}
