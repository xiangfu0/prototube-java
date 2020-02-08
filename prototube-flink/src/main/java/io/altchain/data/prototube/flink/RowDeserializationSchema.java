package io.altchain.data.prototube.flink;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import io.altchain.data.idl.Prototube;
import io.altchain.data.prototube.MessageDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class RowDeserializationSchema extends MessageDeserializer
        implements DeserializationSchema<Row>, Serializable {
  private static final long serialVersionUID = 2L;
  private static final int SECOND_TO_MILLS = 1000;
  private final TypeInformation<Row> type;
  private final RowType headerType;
  private final RowType recordType;
  private final int totalFields;

  private RowDeserializationSchema(
          String payloadClassName,
          Prototube.PrototubeMessageHeader.Builder header,
          AbstractMessage.Builder<?> record,
          RowType headerType,
          RowType recordType,
          RowTypeInfo type) {
    super(payloadClassName, header, record);
    this.headerType = headerType;
    this.recordType = recordType;
    this.type = type;
    this.totalFields = headerType.getFieldCount() + recordType.getFieldCount();
  }

  public static Map.Entry<TableSchema, RowDeserializationSchema> create(String payloadClassName)
          throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    final Prototube.PrototubeMessageHeader.Builder header = Prototube.PrototubeMessageHeader.newBuilder();
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    AbstractMessage.Builder<?> record = (AbstractMessage.Builder) clazz.getMethod("newBuilder").invoke(null);
    Map.Entry<RowType, DataTypes.Field[]> headerSchema = SchemaReflection.headerSchema(header.getDescriptorForType());
    Map.Entry<RowType, DataTypes.Field[]> recordSchema = SchemaReflection.schemaFor(record.getDescriptorForType());
    RowType headerType = headerSchema.getKey();
    RowType recordType = recordSchema.getKey();

    String[] names = Stream.concat(headerType.getFieldNames().stream(), recordType.getFieldNames().stream())
            .toArray(String[]::new);
    DataType[] dataTypes = Stream.concat(Arrays.stream(headerSchema.getValue()), Arrays.stream(recordSchema.getValue()))
            .map(DataTypes.Field::getDataType).toArray(DataType[]::new);
    TableSchema tableSchema = TableSchema.builder().fields(names, dataTypes).build();
    RowTypeInfo rowTypeInfo = new RowTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(dataTypes), names);
    RowDeserializationSchema deserialize = new
            RowDeserializationSchema(payloadClassName, header, record, headerType, recordType, rowTypeInfo);
    return new AbstractMap.SimpleImmutableEntry<>(tableSchema, deserialize);
  }

  @Override
  public Row deserialize(byte[] message) {
    try {
      ByteBuffer buf = ByteBuffer.wrap(message);
      boolean ret = deserializeMessage(buf);
      if (!ret) {
        return null;
      }
    } catch (IOException ignored) {
      return null;
    }

    Object[] fields = new Object[totalFields];
    protoToRow(header, headerType, fields, 0);
    protoToRow(record, recordType, fields, headerType.getFieldCount());
    return Row.of(fields);
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return type;
  }

  private static void protoToRow(MessageOrBuilder msg, RowType schema, Object[] values, int offset) {
    List<Descriptors.FieldDescriptor> fields = msg.getDescriptorForType().getFields();
    for (int i = 0; i < fields.size(); ++i) {
      Object v = msg.getField(fields.get(i));
      values[offset + i] = unwrap(v, schema.getFields().get(i).getType());
    }
  }

  private static Object unwrap(Object v, LogicalType ty) {
    if (v instanceof MessageOrBuilder) {
      RowType nestedType = (RowType) ty;
      Object[] nestedValue = new Object[nestedType.getFieldCount()];
      protoToRow((MessageOrBuilder) v, nestedType, nestedValue, 0);
      return Row.of(nestedValue);
    } else if (v instanceof ByteString) {
      return ((ByteString) v).toByteArray();
    } else if (v instanceof List) {
      List<?> l = (List<?>) v;
      Object[] array = new Object[l.size()];
      ArrayType aty = (ArrayType) ty;
      for (int i = 0; i < l.size(); ++i) {
        array[i] = unwrap(l.get(i), aty.getElementType());
      }
      return array;
    } else if (v instanceof Descriptors.EnumValueDescriptor) {
      return v.toString();
    } else if (ty instanceof TimestampType && v instanceof Long) {
      return new Timestamp((Long) v * SECOND_TO_MILLS);
    } else {
      return v;
    }
  }
}
