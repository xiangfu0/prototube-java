package io.altchain.data.prototube.flink;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.AbstractMap;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

final class SchemaReflection {
  private static final EnumMap<Descriptors.FieldDescriptor.JavaType, DataType> DATATYPE_MAP;
  private static final int SQL_TIMESTAMP_PRECISION = 3;

  private SchemaReflection() {
  }

  static {
    HashMap<Descriptors.FieldDescriptor.JavaType, DataType> m = new HashMap<>();
    m.put(JavaType.BOOLEAN, DataTypes.BOOLEAN());
    m.put(JavaType.INT, DataTypes.INT());
    m.put(JavaType.LONG, DataTypes.BIGINT());
    m.put(JavaType.FLOAT, DataTypes.FLOAT());
    m.put(JavaType.DOUBLE, DataTypes.DOUBLE());
    m.put(JavaType.STRING, DataTypes.STRING());
    m.put(JavaType.BYTE_STRING, DataTypes.BYTES());
    m.put(JavaType.ENUM, DataTypes.STRING());
    DATATYPE_MAP = new EnumMap<>(m);
  }

  static Map.Entry<RowType, Field[]> headerSchema(Descriptors.Descriptor desc) {
    Field[] fields = desc.getFields().stream().map(x -> structFieldFor(x, "_pbtb_"))
            .toArray(Field[]::new);
    RowType ty = (RowType) (DataTypes.ROW(fields).getLogicalType());
    return new AbstractMap.SimpleImmutableEntry<>(ty, fields);
  }

  static Map.Entry<RowType, Field[]> schemaFor(Descriptors.Descriptor desc) {
    Field[] fields = desc.getFields().stream().map(x -> structFieldFor(x, ""))
            .toArray(Field[]::new);
    RowType ty = (RowType) (DataTypes.ROW(fields).getLogicalType());
    return new AbstractMap.SimpleImmutableEntry<>(ty, fields);
  }

  private static Field structFieldFor(Descriptors.FieldDescriptor fd, String prefix) {
    DataType dataType = DATATYPE_MAP.get(fd.getJavaType());
    if (prefix.equals("_pbtb_") && fd.getName().equals("ts")) {
      // Hack to generate event time
      dataType = DataTypes.TIMESTAMP(SQL_TIMESTAMP_PRECISION).bridgedTo(java.sql.Timestamp.class);
    } else if (dataType == null) {
      Field[] fields = fd.getMessageType().getFields().stream()
              .map(x -> structFieldFor(x, prefix)).toArray(Field[]::new);
      dataType = DataTypes.ROW(fields);
    }
    DataType ty = fd.isRepeated() ? DataTypes.ARRAY(dataType) : dataType;
    boolean nullable = !fd.isRequired() && !fd.isRepeated();
    if (nullable) {
      ty = ty.nullable();
    }
    return DataTypes.FIELD(prefix + fd.getName(), ty);
  }
}
