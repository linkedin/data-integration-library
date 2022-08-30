// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * This utility class aims to simplify the structure manipulation of JsonSchema.
 *
 * At the same time, we are deprecating JsonSchema class
 */
public class SchemaBuilder {
  final private static JsonElement JSON_NULL_STRING = new JsonPrimitive("null");

  final public static int UNKNOWN = 0;
  final public static int RECORD = 1;
  final public static int ARRAY = 2;
  final public static int PRIMITIVE = 3;

  final private static ImmutableMap<String, Integer> COMMON = ImmutableMap.of(
      KEY_WORD_RECORD, RECORD,
      KEY_WORD_OBJECT, RECORD,
      KEY_WORD_ARRAY, ARRAY,
      KEY_WORD_PRIMITIVE, PRIMITIVE,
      KEY_WORD_UNKNOWN, UNKNOWN);

  private int type = UNKNOWN;
  private String name;
  private String primitiveType = null;
  private boolean isNullable = true;
  List<SchemaBuilder> elements = new ArrayList<>();

  /**
   * create a root builder with a list of children
   * @param type the type of builder element
   * @param isNullable the nullability of builder element
   * @param elements the child elements
   */
  public SchemaBuilder(final int type, final boolean isNullable, List<SchemaBuilder> elements) {
    this("root", type, isNullable, elements);
  }

  /**
   * create a builder with a list of children
   * @param name the element name
   * @param type the type of builder element
   * @param isNullable the nullability of builder element
   * @param elements the child elements
   */
  public SchemaBuilder(final String name, final int type, final boolean isNullable, List<SchemaBuilder> elements) {
    this.name = name;
    this.type = type;
    this.isNullable = isNullable;
    this.elements.addAll(elements);
  }

  /**
   * create a builder with a single child
   * @param name the element name
   * @param type the type of builder element
   * @param isNullable the nullability of builder element
   * @param element the child element
   */
  public SchemaBuilder(final String name, final int type, final boolean isNullable, SchemaBuilder element) {
    this.name = name;
    this.type = type;
    this.isNullable = isNullable;
    this.elements.add(element);
  }

  /**
   * This is main method to parse an Avro schema, and make it into a builder.
   * The builder can then be used to produce schema strings in other syntax
   *
   * @param json the JsonObject object representing an Avro schema
   * @return a SchemaBuilder
   */
  public static SchemaBuilder fromAvroSchema(JsonElement json) {
    assert json.isJsonObject();
    return new SchemaBuilder(RECORD, json.getAsJsonObject().get(KEY_WORD_FIELDS));
  }

  /**
   * This is the main method to parse a Json sample data and infer the schema from
   * the sample. The method uses the {@link Generator} as a helper in doing so.
   *
   * The inferred SchemaBuilder can then be used to produce strings in other syntax.
   *
   * @param data the Json data sample
   * @return the inferred SchemaBuilder
   */
  public static SchemaBuilder fromJsonData(JsonElement data) {
    return new Generator(data).getSchemaBuilder();
  }

  /**
   * This is the main method to parse a Json sample data and infer the schema from
   * the sample. The method uses the {@link Generator} as a helper in doing so.
   *
   * The inferred SchemaBuilder can then be used to produce strings in other syntax.
   *
   * @param data the Json data sample
   * @return the inferred SchemaBuilder
   */
  public static SchemaBuilder fromJsonData(String data) {
    return fromJsonData(GSON.fromJson(data, JsonElement.class));
  }


  /**
   * This is the main method to convert a schema definition in Json Schema syntax,
   * and product a SchemaBuilder object. The SchemaBuilder object can then be used
   * to product a string in Avro schema syntax.
   *
   * The input JsonSchema can be in 3 forms:
   *  1. an anonymous object with a list columns, there is no top level type element
   *  2. an anonymous array with a list of columns, there is a top level array type element
   *  3. an anonymous object with a list of columns, there is top level object type element
   *
   * @param jsonSchema the schema of a Json dataset in the Json Schema syntax
   * @return a SchemaBuilder
   */
  public static SchemaBuilder fromJsonSchema(JsonObject jsonSchema) {
    int rootType = getType(jsonSchema);
    return new SchemaBuilder(rootType == UNKNOWN ? RECORD : rootType, true, fromJsonComplexSchema(jsonSchema));
  }

  /**
   * Override version to accept Json Schema string
   *
   * @param jsonSchema the schema of a Json dataset in the Json Schema syntax
   * @return a SchemaBuilder
   */
  public static SchemaBuilder fromJsonSchema(String jsonSchema) {
    return fromJsonSchema(GSON.fromJson(jsonSchema, JsonObject.class));
  }

  /**
   * Parse a Json Schema and convert the elements to a SchemaBuilder list
   * @param complexSchema a schema in Json Schema syntax
   * @return the converted list of elements
   */
  private static List<SchemaBuilder> fromJsonComplexSchema(JsonObject complexSchema) {
    switch (getType(complexSchema)) {
      case ARRAY:
        List<SchemaBuilder> elements = fromJsonComplexSchema(complexSchema.get(KEY_WORD_ITEMS).getAsJsonObject());
        if (getType(complexSchema.get(KEY_WORD_ITEMS).getAsJsonObject()) == PRIMITIVE) {
          SchemaBuilder element = new SchemaBuilder(KEY_WORD_ARRAY_ITEM, PRIMITIVE, true, new ArrayList<>());
          element.setNullable(checkNullable(complexSchema.get(KEY_WORD_ITEMS).getAsJsonObject()));
          element.setPrimitiveType(getNestedType(complexSchema.get(KEY_WORD_ITEMS).getAsJsonObject()));
          elements.add(element);
        } else if (getType(complexSchema.get(KEY_WORD_ITEMS).getAsJsonObject()) == RECORD) {
          List<SchemaBuilder> singletonList = new ArrayList<>();
          singletonList.add(new SchemaBuilder(KEY_WORD_ARRAY_ITEM, RECORD, true, elements));
          return singletonList;
        }
        return elements;
      case RECORD:
        return fromJsonComplexSchema(complexSchema.get(KEY_WORD_PROPERTIES).getAsJsonObject());
      case PRIMITIVE:
        return new ArrayList<>();
      default:
        return fromJsonObjectSchema(complexSchema.getAsJsonObject());
    }
  }

  /**
   * Process a untyped Json schema object as a set of columns. If a Json
   * schema object has no "type" element, the object is treated as a table
   * or sub-table, and each entry of the JsonObject is treated as a column.
   *
   * @param jsonObject a untyped JsonObject of columns
   * @return a list of columns each defined as a SchemaBuilder
   */
  private static List<SchemaBuilder> fromJsonObjectSchema(JsonObject jsonObject) {
    List<SchemaBuilder> columns = new ArrayList<>();
    jsonObject.entrySet().iterator().forEachRemaining(x -> {
      boolean isNullable = checkNullable(x.getValue().getAsJsonObject());
      int type = getType(x.getValue().getAsJsonObject());
      String primitiveType = getNestedType(x.getValue().getAsJsonObject());
      SchemaBuilder column = new SchemaBuilder(x.getKey(), type, true,
          fromJsonComplexSchema(x.getValue().getAsJsonObject()));
      column.setNullable(isNullable);
      if (type == PRIMITIVE) {
        column.setPrimitiveType(primitiveType);
      }
      columns.add(column);
    });
    return columns;
  }

  /**
   * Hidden constructor that is called by public static function only
   * @param type the type of the schema element
   * @param json the Json presentation of the schema element
   */
  private SchemaBuilder(final int type, JsonElement json) {
    this("root", type, json);
  }

  /**
   * Hidden constructor that is called by internal parsing functions only
   * @param name the name of the schema element
   * @param type the type of the schema element
   * @param json the Json presentation of the schema element
   */
  private SchemaBuilder(final String name, final int type, final JsonElement json) {
    parseAvroSchema(name, type, json);
  }

  /**
   * Hidden initialization function
   * @param name the name of the schema element
   * @param type the type of the schema element
   * @param columnDef the Json presentation of the schema element
   */
  private void parseAvroSchema(final String name, final int type, final JsonElement columnDef) {
    this.type = type == UNKNOWN ? getType(columnDef) : type;
    this.isNullable = !name.equals("root") && columnDef.isJsonObject() && checkNullable(columnDef.getAsJsonObject());
    this.name = name;
    switch (this.type) {
      case RECORD:
        if (name.equals("root")) {
          for (JsonElement field: columnDef.getAsJsonArray()) {
            elements.add(new SchemaBuilder(field.getAsJsonObject().get(KEY_WORD_NAME).getAsString(), UNKNOWN, field));
          }
        } else {
          for (JsonElement field: getFields(columnDef.getAsJsonObject())) {
            elements.add(new SchemaBuilder(field.getAsJsonObject().get(KEY_WORD_NAME).getAsString(), UNKNOWN, field));
          }
        }
        break;
      case ARRAY:
        elements.add(new SchemaBuilder("arrayItem", UNKNOWN, getItems(columnDef.getAsJsonObject())));
        break;
      default: //PRIMITIVE
        if(columnDef.isJsonObject()) {
          this.primitiveType = getNestedType(columnDef.getAsJsonObject());
        } else if (columnDef.isJsonPrimitive()) {
          this.primitiveType = columnDef.getAsString();
        } else {
          throw new RuntimeException("Primitive type definition can be only a simple string or a JSON object with a nested type.");
        }
        break;
    }
  }

  /**
   * Get the schema element type
   * @param json the Json presentation of the schema element
   * @return the schema element type
   */
  private static int getType(JsonElement json) {
    if (json.isJsonPrimitive()) {
      return getType(json.getAsString());
    }
    return getType(getNestedType(json.getAsJsonObject()));
  }

  /**
   * Get the schema element type string from a straight or a unionized schema element
   * @param json the Json presentation of the schema element
   * @return the schema element type string
   */
  private static String getNestedType(JsonObject json) {
    JsonElement type = json.get(KEY_WORD_TYPE);

    if (type == null) {
      return KEY_WORD_UNKNOWN;
    }

    if (type.isJsonPrimitive()) {
      return type.getAsString();
    }

    if (type.isJsonObject()) {
      if (type.getAsJsonObject().has(KEY_WORD_SOURCE_TYPE)) {
        return type.getAsJsonObject().get(KEY_WORD_SOURCE_TYPE).getAsString();
      }
      return type.getAsJsonObject().get(KEY_WORD_TYPE).getAsString();
    }

    if (type.isJsonArray()) {
      Set<JsonElement> items = new HashSet<>();
      type.getAsJsonArray().iterator().forEachRemaining(items::add);
      items.remove(JSON_NULL_STRING);
      JsonElement trueType = items.iterator().next();
      if (trueType.isJsonPrimitive()) {
        return trueType.getAsString();
      } else if (trueType.isJsonObject()) {
        if (trueType.getAsJsonObject().has(KEY_WORD_SOURCE_TYPE)) {
          return trueType.getAsJsonObject().get(KEY_WORD_SOURCE_TYPE).getAsString();
        } else if (trueType.getAsJsonObject().has(KEY_WORD_TYPE)) {
          return trueType.getAsJsonObject().get(KEY_WORD_TYPE).getAsString();
        }
      }
    }
    return KEY_WORD_UNKNOWN;
  }

  /**
   * Map a string type to internal presentation of integer type
   * @param type the schema element type string
   * @return the schema element type integer
   */
  private static int getType(String type) {
    return COMMON.getOrDefault(type, PRIMITIVE);
  }

  /**
   * Check if an schema element is nullable, this is for AVRO schema parsing only
   * @param json the Json presentation of the schema element
   * @return nullability
   */
  private static boolean checkNullable(JsonObject json) {
    JsonElement type = json.get(KEY_WORD_TYPE);
    if (type.isJsonPrimitive()) {
      return type.equals(JSON_NULL_STRING);
    }

    if (type.isJsonObject()) {
      return type.getAsJsonObject().get(KEY_WORD_TYPE).equals(JSON_NULL_STRING);
    }

    if (type.isJsonArray()) {
      return type.getAsJsonArray().contains(JSON_NULL_STRING);
    }

    return true;
  }

  /**
   * Parse out the "fields" from an Avro schema
   *
   * If the schema is a nullable record, the type would be a union, and fields
   * data element would be hidden in an JsonArray. In such cases, we will get the
   * fields from the first non-null type.
   *
   * TODO: unions of more than 1 non-null types are not supported right now
   *
   * @param record the "record" schema element
   * @return the fields of the record
   */
  private JsonArray getFields(JsonObject record) {
    if (record.has(KEY_WORD_FIELDS)) {
      return record.get(KEY_WORD_FIELDS).getAsJsonArray();
    }

    if (record.get(KEY_WORD_TYPE).isJsonObject()) {
      return record.get(KEY_WORD_TYPE).getAsJsonObject().get(KEY_WORD_FIELDS).getAsJsonArray();
    }

    if (record.get(KEY_WORD_TYPE).isJsonArray()) {
      Set<JsonElement> union = new HashSet<>();
      record.get(KEY_WORD_TYPE).getAsJsonArray().iterator().forEachRemaining(union::add);
      union.remove(JSON_NULL_STRING);
      if (union.iterator().hasNext()) {
        JsonElement next = union.iterator().next();
        if (next.isJsonObject() && next.getAsJsonObject().has(KEY_WORD_FIELDS)) {
          return next.getAsJsonObject().get(KEY_WORD_FIELDS).getAsJsonArray();
        }
      }
    }

    return new JsonArray();
  }

  /**
   * Parse out the array items in an Avro schema, current an array item can be a record,
   * a primitive, a null, or a union of any two of them. However, the union shall not be
   * more than 2 types.
   *
   * @param columnDef the "array" schema element
   * @return the array item
   */
  private JsonElement getItems(JsonObject columnDef) {
    if (columnDef.get(KEY_WORD_TYPE).isJsonObject()) {
      return columnDef.get(KEY_WORD_TYPE).getAsJsonObject().get(KEY_WORD_ITEMS);
    } if (columnDef.get(KEY_WORD_TYPE).isJsonArray()) {
      Set<JsonElement> union = new HashSet<>();
      columnDef.get(KEY_WORD_TYPE).getAsJsonArray().iterator().forEachRemaining(union::add);
      union.remove(JSON_NULL_STRING);
      return union.iterator().next().getAsJsonObject().get(KEY_WORD_ITEMS);
    } else {
      throw new RuntimeException("Getting primitive while JSON array, for union type, or JSON object, for single type, are expected.");
    }
  }

  /**
   * Build into a Json schema definition that can be converted to a string
   * @return a Json schema
   */
  public JsonObject buildJsonSchema() {
    return buildJsonSchema(false);
  }

  /**
   * Build into a Json schema definition that can be converted to a string
   * @param includeRootType whether to include the root type element
   * @return a Json schema
   */
  public JsonObject buildJsonSchema(boolean includeRootType) {
    if (this.type == RECORD || type == ARRAY) {
      JsonObject fields = new JsonObject();

      if (type == ARRAY && elements.size() == 1 && elements.get(0).getType() == PRIMITIVE) {
        fields = elements.get(0).buildJsonSchema(includeRootType);
      } else {
        for (SchemaBuilder field : elements) {
          if (field.getName().equals(KEY_WORD_ARRAY_ITEM) && elements.size() == 1) {
            fields = field.buildJsonSchema(includeRootType);
          } else {
            fields.add(field.getName(), field.buildJsonSchema(includeRootType));
          }
        }
      }

      if (name.equals(KEY_WORD_ROOT) && !includeRootType) {
        return fields;
      }

      if (type == ARRAY) {
        JsonObject array = JsonUtils.createAndAddProperty(KEY_WORD_TYPE, KEY_WORD_ARRAY);
        array.add(KEY_WORD_ITEMS, fields);
        return array;
      }

      if (name.equals(KEY_WORD_ARRAY_ITEM)) {
        JsonObject arrayItem = JsonUtils.createAndAddProperty(KEY_WORD_TYPE, KEY_WORD_OBJECT);
        arrayItem.add(KEY_WORD_PROPERTIES, fields);
        return arrayItem;
      }

      JsonObject subTable = JsonUtils.createAndAddProperty(KEY_WORD_TYPE, KEY_WORD_OBJECT);
      subTable.add(KEY_WORD_PROPERTIES, fields);
      return subTable;
    } else {
        String nullableType = isNullable && !primitiveType.equalsIgnoreCase(KEY_WORD_NULL)
            ? KEY_WORD_NULLABLE + primitiveType : primitiveType;
        return addPrimitiveSchemaType(JsonElementTypes.valueOf(nullableType.toUpperCase()));
    }
  }

  /**
   * Build into a Avro flavored, but not true Avro, schema that can be fed into
   * Json2Avro converter
   *
   * @return Json presentation of the Avro flavored schema
   */
  public JsonElement buildAltSchema() {
    return buildAltSchema(new HashMap<>(), false, null, null, false);
  }

  public JsonElement buildAltSchema(boolean recordTypeNullable) {
    return buildAltSchema(new HashMap<>(), false, null, null, false, recordTypeNullable);
  }

  /**
   * For backward compatibility.
   * i.e. no explicit nullable setting for sub-tables (RECORD type)
   */
  public JsonElement buildAltSchema(Map<String, String> defaultTypes,
      boolean enableCleansing,
      String pattern,
      String replacement,
      boolean nullable) {
    return buildAltSchema(defaultTypes, enableCleansing, pattern, replacement, nullable, false);
  }

  /**
   * Build into a Avro flavored, but not true  Avro, schema that can be fed into
   * Json2Avro converter. Along the way, schema names are cleansed if special characters
   * are present.
   *
   * This method is called recursively, therefore the return element can be a JsonArray,
   * for root schema, and JsonObject for child schema elements, primitive columns,
   * arrays, and sub-tables.
   *
   * @param defaultTypes the default data types are used to explicitly assign types that cannot be inferred
   *                     correctly
   * @param enableCleansing if cleansing is enabled, field names will be checked and special characters will
   *                        be replaced with a replacement character
   * @param pattern the search pattern of schema cleansing
   * @param replacement the replacement string for schema cleansing
   * @param nullable whether to force output all columns as nullable
   * @param recordTypeNullable whether RECORD type can be explicitly set as nullable
   * @return the Avro flavored schema definition
   */
  public JsonElement buildAltSchema(Map<String, String> defaultTypes,
      boolean enableCleansing,
      String pattern,
      String replacement,
      boolean nullable,
      boolean recordTypeNullable) {
    JsonObject nestedType = new JsonObject();
    if (this.type == RECORD
        || (this.type == ARRAY && this.elements.size() > 1)) {
      JsonArray fields = new JsonArray();
      for (SchemaBuilder field : elements) {
        fields.add(field.buildAltSchema(defaultTypes, enableCleansing, pattern, replacement, nullable, recordTypeNullable));
      }
      if (name.equals("root") || type == ARRAY) {
        return fields;
      }
      nestedType.addProperty(KEY_WORD_TYPE, KEY_WORD_RECORD);
      nestedType.addProperty(KEY_WORD_NAME, this.name);
      nestedType.add(KEY_WORD_VALUES, fields);
    } else if (this.type == ARRAY) {
      nestedType.addProperty(KEY_WORD_TYPE, KEY_WORD_ARRAY);
      nestedType.addProperty(KEY_WORD_NAME, this.name);
      if (this.elements.get(0).getType() == PRIMITIVE) {
        nestedType.addProperty(KEY_WORD_ITEMS, this.elements.get(0).getPrimitiveType());
      } else {
        nestedType.add(KEY_WORD_ITEMS,
            this.elements.get(0).buildAltSchema(defaultTypes, enableCleansing, pattern, replacement, nullable, recordTypeNullable));
      }
    } else {
        nestedType.addProperty(KEY_WORD_TYPE, defaultTypes.getOrDefault(this.getName(), this.primitiveType));
    }

    JsonObject column = new JsonObject();
    column.addProperty(KEY_WORD_COLUMN_NAME, enableCleansing ? name.replaceAll(pattern, replacement) : name);
    // no explicit nullable setting for sub-tables, unless explicitly requested
    if (this.type != RECORD || recordTypeNullable) {
      column.addProperty(KEY_WORD_DATA_IS_NULLABLE, nullable || this.isNullable);
    }
    column.add(KEY_WORD_DATA_TYPE, nestedType);
    return column;
  }

  public boolean isNullable() {
    return this.isNullable;
  }

  public SchemaBuilder setNullable(boolean nullable) {
    this.isNullable = nullable;
    return this;
  }

  public String getName() {
    return this.name;
  }

  public SchemaBuilder setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Retrieve the primitive type string, and convert null to string
   *
   * @return the primitive type string
   */
  public String getPrimitiveType() {
    return isNullable && primitiveType.equalsIgnoreCase(KEY_WORD_NULL)
        ? KEY_WORD_STRING : this.primitiveType;
  }

  /**
   * Set the primitive type and return an object for chained calls
   *
   * @param primitiveType the primitive type string
   * @return the builder object itself
   */
  public SchemaBuilder setPrimitiveType(String primitiveType) {
    if (primitiveType.equalsIgnoreCase(KEY_WORD_NULL)) {
      this.isNullable = true;
      this.primitiveType = KEY_WORD_STRING;
    } else {
      this.primitiveType = primitiveType;
    }
    return this;
  }

  public int getType() {
    return type;
  }

  /**
   * Add a type description to the schema element
   *
   * Primitive schema item types can have values like following:
   * {"type", "null"}
   * {"type", "string"}
   * {"type", ["string", "null"]}
   *
   * @param itemType the type of this JsonSchema item
   * @return the modified JsonSchema item with the specified type
   */
  public JsonObject addPrimitiveSchemaType(JsonElementTypes itemType) {
    if (itemType.isNull()) {
      return JsonUtils.createAndAddProperty(KEY_WORD_TYPE, "null");
    }

    if (itemType.isNullable()) {
      JsonArray typeArray = new JsonArray();
      typeArray.add(itemType.reverseNullability().toString());
      typeArray.add("null");
      return JsonUtils.createAndAddElement(KEY_WORD_TYPE, typeArray);
    }

    return JsonUtils.createAndAddProperty(KEY_WORD_TYPE, itemType.toString());
  }

  /**
   * This utility class helps parse Json data and infer schema.
   *
   * Json data have a very loose schema definition, data elements can have incomplete structure from record
   * to record. In order properly infer a complete schema, a batch of records is necessary.
   *
   * TODO: to be able to parse a stream of Json records because event a batch of records sometimes
   * are insufficient.
   *
   */
  public static class Generator {
    final private static Logger LOGGER = Logger.getLogger(Generator.class);
    private JsonElement data;
    private boolean pivoted = false;

    public Generator(JsonElement data) {
      this.data = data;
    }

    public Generator(JsonElement data, boolean pivoted) {
      this.data = data;
      this.pivoted = pivoted;
    }

    /**
     * Given a Generator initialized with a Json object, a JsonObject or JsonArray,
     * the function infers the schema using the best guess and stores it in a
     * SchemaBuilder, which can then be used to produce the schema strings in
     * various formats.
     *
     * @return the SchemaBuilder that store the schema structure
     */
    public SchemaBuilder getSchemaBuilder() {
      List<SchemaBuilder> elements = new ArrayList<>();
      if (data.isJsonObject()) {
        data.getAsJsonObject().entrySet().iterator().forEachRemaining(x -> elements.add(
            new Generator(x.getValue(), true).getSchemaBuilder().setName(x.getKey())));
        return new SchemaBuilder(SchemaBuilder.RECORD, true, elements);
      }

      if (data.isJsonPrimitive() || data.isJsonNull()) {
        return new SchemaBuilder(KEY_WORD_UNKNOWN, SchemaBuilder.PRIMITIVE, data.isJsonNull(), new ArrayList<>())
            .setPrimitiveType(inferPrimitiveType(data));
      }

      if (!pivoted) {
        data = pivotJsonArray(data.getAsJsonArray());
      }

      if (data.getAsJsonArray().size() > 0 && data.getAsJsonArray().get(0).isJsonArray()) {
        data.getAsJsonArray().iterator().forEachRemaining(x -> elements.add(inferColumnSchemaFromSample(x.getAsJsonArray())));
      } else {
        elements.add(new SchemaBuilder(KEY_WORD_UNKNOWN,
            SchemaBuilder.PRIMITIVE, data.getAsJsonArray().size() == 0, new ArrayList<>())
            .setPrimitiveType(data.getAsJsonArray().size() == 0 ? KEY_WORD_NULL
                : inferPrimitiveType(data.getAsJsonArray().get(0))));
      }

      if (!pivoted) {
        return new SchemaBuilder(SchemaBuilder.RECORD, true, elements);
      }

      return new SchemaBuilder(KEY_WORD_UNKNOWN, SchemaBuilder.ARRAY, elements.get(0).isNullable(), elements);
    }

    /**
     * This function infers schema from a sample that is structured as a JsonArray and stores
     * the schema in SchemaBuilder, which can then be appended to higher level schema as
     * child element.
     *
     * The sample data input should be data from the same column.
     *
     * @param data sample data as JsonArray
     * @return the inferred schema
     */
    private SchemaBuilder inferColumnSchemaFromSample(JsonArray data) {
      // if it is a blank array
      if (data.size() == 0) {
        return new SchemaBuilder(KEY_WORD_UNKNOWN, SchemaBuilder.PRIMITIVE, true, new ArrayList<>())
            .setPrimitiveType(KEY_WORD_NULL);
      }

      JsonElementTypes itemType = JsonElementTypes.getTypeFromMultiple(data);

      // if it is a sub table array, or an array of arrays
      if (itemType.isObject()) {
        return inferSchemaFromKVPairs(data);
      }

      if (itemType.isArray()) {
        return inferSchemaFromNestedArray(data);
      }

      // if it is an array of primitives
      return new SchemaBuilder(KEY_WORD_UNKNOWN, SchemaBuilder.PRIMITIVE,
          itemType.isNullable(), new ArrayList<>()).setPrimitiveType(itemType.getAltName());
    }

    /**
     * This function takes an array of name value pairs and infer their schema
     *
     * @param data A Json array of objects
     * @return inferred Schema Builder
     */
    private SchemaBuilder inferSchemaFromKVPairs(JsonArray data) {
      // ignore potentially null values at the beginning
      int i = 0;
      while (i < data.size() && (isEmpty(data.get(i))
          || isEmpty(data.get(i).getAsJsonObject().entrySet().iterator().next().getValue()))) {
        ++i;
      }

      // for placeholder type of fields, all values will be null, i will be larger than size
      // in this case, we just reset i to 0
      Map.Entry<String, JsonElement> dataEntry = data.get(i >= data.size() ? 0 : i)
          .getAsJsonObject().entrySet().iterator().next();
      String memberKey = dataEntry.getKey();
      JsonArray objectData = getValueArray(data);

      // The value has a nested sub-table
      if (isSubTable(dataEntry.getValue())) {
        return new Generator(objectData).getSchemaBuilder().setName(memberKey);
      }

      JsonElementTypes subType = JsonElementTypes.getTypeFromMultiple(objectData);

      // The value has an array
      if (subType.isArray()) {
        return new SchemaBuilder(memberKey, SchemaBuilder.ARRAY, subType.isNullable(),
            inferSchemaFromNestedArray(objectData));
      }

      // The values primitive
      return new SchemaBuilder(memberKey, SchemaBuilder.PRIMITIVE, subType.isNullable(),
          new ArrayList<>()).setPrimitiveType(subType.getAltName());
    }

    /**
     * This function takes an array of rows and infer the row schema column by column
     *
     * @param data an array of rows with 1 or more columns
     * @return the inferred Schema Builder
     */
    private SchemaBuilder inferSchemaFromNestedArray(JsonArray data) {
      // strip off one layer of array because data is like
      // [[{something}],[{something}]]
      JsonArray arrayData = new JsonArray();
      for (JsonElement element: data) {
        if (element.isJsonNull() || element.getAsJsonArray().size() == 0) {
          arrayData.add(JsonNull.INSTANCE);
        } else {
          arrayData.addAll(element.getAsJsonArray());
        }
      }
      JsonElementTypes subType = JsonElementTypes.getTypeFromMultiple(arrayData);

      if (subType.isObject()) {
        return new SchemaBuilder(KEY_WORD_UNKNOWN, SchemaBuilder.RECORD, subType.isNullable(),
            inferColumnSchemaFromSample(arrayData));
      }

      return new SchemaBuilder(KEY_WORD_UNKNOWN, SchemaBuilder.PRIMITIVE, subType.isNullable(),
          new ArrayList<>()).setPrimitiveType(subType.getAltName());
    }

    /**
     * Infer schema from a primitive
     * @param data the primitive data value
     * @return a string of the primitive type
     */
    private String inferPrimitiveType(JsonElement data) {
      assert data.isJsonPrimitive() || data.isJsonNull();
      return data.isJsonNull() ? KEY_WORD_NULL
          : data.toString().matches("^\".*\"$") ? KEY_WORD_STRING
              : data.getAsString().toLowerCase().matches("(true|false)") ? KEY_WORD_BOOLEAN
                  : inferNumeric(data.getAsString());
    }

    /**
     * Infer whether the numeric value is an integer. We are not differentiating other
     * numeric types since Json treat them the same way
     * @param value the numeric value
     * @return integer or number
     */
    private String inferNumeric(String value) {
      try {
        Integer.parseInt(value);
      } catch (Exception e) {
        return KEY_WORD_NUMBER;
      }
      return KEY_WORD_INTEGER;
    }

    /**
     * This function takes only the value part of the key value pair array
     *
     * @param kvArray an array of KV pairs
     * @return an array contains only the value part
     */
    private JsonArray getValueArray(JsonArray kvArray) {
      assert kvArray.size() > 0;

      int i = 0;
      while (kvArray.get(i).isJsonNull()) {
        ++i;
      }
      String key = kvArray.get(i).getAsJsonObject().entrySet().iterator().next().getKey();
      JsonArray valueArray = new JsonArray();
      for (JsonElement element: kvArray) {
        if (element.isJsonNull()) {
          valueArray.add(JsonNull.INSTANCE);
        } else {
          valueArray.add(element.getAsJsonObject().get(key));
        }
      }
      return valueArray;
    }

    /**
     * Pivot JsonArray so that all values of the same column can be parsed altogether.
     * This is important for nullability analysis. By taking only 1 record from an JsonArray
     * to derive schema for the whole dataset, we would be seeing part of the types of a nullable
     * column.
     *
     * The input can be:
     *   1. array of JsonObjects
     *   2. array of Primitives
     *   3. array of Arrays
     *
     * The input cannot be:
     *   4. array of mixed types
     *
     * TODO: to handle union types, this requires further work
     *
     * @param data a JsonArray of records
     * @return an JsonArray of JsonArrays
     */
    JsonArray pivotJsonArray(JsonArray data) {
      int i = 0;
      JsonArray pivotedArray = new JsonArray();
      Map<String, Integer> columnIndex = new HashMap<>();

      while (i < data.size()
          && (data.get(i).isJsonNull() || isEmpty(data.get(i)))) {
        ++i;
      }

      // in case data has no records, or data has only blank records, then no action and
      // return a blank pivoted array.
      if (i >= data.size()) {
        return pivotedArray;
      }

      JsonElementTypes elementType = getJsonElementType(data.get(i));

      if (elementType == JsonElementTypes.PRIMITIVE) {
        return data;
      }

      for (JsonElement row: data) {
        if (!row.isJsonObject()) {
          LOGGER.error("Array of Arrays is not supported");
          return new JsonArray();
        }
        for (Map.Entry<String, JsonElement> entry : row.getAsJsonObject().entrySet()) {
          if (!columnIndex.containsKey(entry.getKey())) {
            pivotedArray.add(new JsonArray());
            columnIndex.put(entry.getKey(), columnIndex.size());
          }
        }
      }

      for (JsonElement element: data) {
        if (element.isJsonNull() || isEmpty(element)) {
          for (i = 0; i < columnIndex.size(); ++i) {
            pivotedArray.get(i).getAsJsonArray().add(JsonNull.INSTANCE);
          }
        } else {
          // each element might have columns in different order,
          // and all elements don't have the same columns
          Preconditions.checkState(elementType == JsonElementTypes.OBJECT);
          for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
            JsonObject temp = new JsonObject();
            temp.add(entry.getKey(), entry.getValue());
            if (columnIndex.get(entry.getKey()) != null && pivotedArray.size() > columnIndex.get(entry.getKey())) {
              pivotedArray.get(columnIndex.get(entry.getKey())).getAsJsonArray().add(temp);
            } else {
              pivotedArray.add(new JsonArray());
              columnIndex.put(entry.getKey(), columnIndex.size());
            }
          }
        }
      }
      return pivotedArray;
    }

    boolean isSubTable(JsonElement data) {
      return data.isJsonObject() && data.getAsJsonObject().entrySet().size() > 0;
    }

    /**
     * Classifies an Json element to 4 high level data types, but doesn't identify further
     * detailed types of primitives
     *
     * @param jsonElement a Json element
     * @return ARRAY, OBJECT, NULL, or PRIMITIVE
     */
    JsonElementTypes getJsonElementType(JsonElement jsonElement) {
      if (jsonElement.isJsonPrimitive()) {
        return JsonElementTypes.PRIMITIVE;
      } else if (jsonElement.isJsonNull()) {
        return JsonElementTypes.NULL;
      } else if (jsonElement.isJsonObject()) {
        return JsonElementTypes.OBJECT;
      } else {
        return JsonElementTypes.ARRAY;
      }
    }

    /**
     * in real world Json strings, empty element can be presented in different forms
     *
     * @param data input data to test
     * @return if data represent an empty object
     *
     */
    boolean isEmpty(JsonElement data) {
      return data == null || data.isJsonNull() || (data.isJsonObject() && data.toString().equals("{}")) || (
          data.isJsonArray() && data.toString().equals("[]")) || (data.isJsonPrimitive() && StringUtils.isEmpty(
          data.getAsString()));
    }
  }
}
