package io.altchain.data.prototube.spark;

import io.altchain.data.idl.test.Test.TestStruct;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RecordDeserializerTest {
  @Test
  public void testDeserializeProtoToRow() {
    TestStruct testTemp = TestStruct.newBuilder()
            .setIntValue(5)
            .setEnumValue(TestStruct.TestEnum.ENUM_2)
            .build();

    final StructType schema =  SchemaReflection.schemaFor(testTemp.getDescriptorForType());
    Object[] values = new Object[schema.size()];
    RecordDeserializer.protoToRow(testTemp, schema, values, 0);
    System.out.println(Arrays.toString(values));
    Assert.assertEquals(5L, values[0]);
    Assert.assertEquals("ENUM_2", values[1]);
  }
}
