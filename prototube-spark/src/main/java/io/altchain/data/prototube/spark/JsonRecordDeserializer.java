package io.altchain.data.prototube.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class JsonRecordDeserializer {

  private final AbstractMessage.Builder record;
  private final StructType recordSchema;
  private Map<String, String> fieldMap;

  public JsonRecordDeserializer(String payloadClassName, Map<String, String> fieldMap)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    this.record = (AbstractMessage.Builder) clazz.getMethod("newBuilder").invoke(null);
    this.recordSchema = SchemaReflection.schemaFor(record.getDescriptorForType());
    this.fieldMap = fieldMap;
  }

  private static Object unwrap(Map<String, Integer> columnIndex, JsonNode v,
                               Map<String, String> fieldMap,
                               AbstractMessage.Builder record, DataType ty) {

    switch (v.getNodeType()) {
      case ARRAY:
        Object[] array = new Object[v.size()];
        ArrayType aty = (ArrayType) ty;
        for (int i = 0; i < v.size(); ++i) {
          array[i] = unwrap(columnIndex, v.get(i), fieldMap, record, aty.elementType());
        }
        return array;
      case OBJECT:
        StructType nestedType = (StructType) ty;
        Object[] nestedValue = new Object[nestedType.size()];
        jsonNodeToRow(columnIndex, v, fieldMap, record, nestedType, nestedValue);
        return RowFactory.create(nestedValue);
      case NUMBER:
      case STRING:
      default:
        final String sparkDataType = ty.typeName();
        switch (sparkDataType) {
          case "boolean":
            return v.asBoolean();
          case "integer":
            return v.asInt();
          case "long":
            return v.asLong();
          case "float":
          case "double":
            return v.asDouble();
          case "string":
            return v.asText();
          case "binary":
            try {
              return v.binaryValue();
            } catch (IOException e) {
              throw new RuntimeException("Failed to parse jsonValue to binary value: " + v);
            }
          default:
            return v.textValue();
        }
    }
  }

  private static void jsonNodeToRow(Map<String, Integer> columnIndex, JsonNode jsonNode,
                                    Map<String, String> fieldMap,
                                    AbstractMessage.Builder record, StructType schema, Object[] values) {
    List<Descriptors.FieldDescriptor> fields = record.getDescriptorForType().getFields();
    List<StructField> types = scala.collection.JavaConversions.seqAsJavaList(schema);
    for (int i = 0; i < fields.size(); ++i) {
      final FieldDescriptor fieldDescriptor = fields.get(i);
      String jsonFieldName = fieldDescriptor.getName();
      if (fieldMap.containsKey(fieldDescriptor.getName())) {
        jsonFieldName = fieldMap.get(fieldDescriptor.getName());
      }
      final Integer colIdx = columnIndex.get(jsonFieldName);
      if (colIdx == null) {
        values[i] = null;
        continue;
      }
      final JsonNode fieldJsonNode = jsonNode.get(colIdx);
      values[i] = unwrap(columnIndex, fieldJsonNode, fieldMap, record, types.get(i).dataType());
    }
  }

  public StructType schema() {
    StructField[] fields = new StructField[recordSchema.fields().length];
    System.arraycopy(recordSchema.fields(), 0, fields, 0, recordSchema.fields().length);
    return new StructType(fields);
  }

  public Row deserialize(Map<String, Integer> columnIndex, JsonNode jsonNode) {
    Object[] fields = new Object[recordSchema.size()];
    jsonNodeToRow(columnIndex, jsonNode, fieldMap, record, recordSchema, fields);
    return RowFactory.create(fields);
  }
}
