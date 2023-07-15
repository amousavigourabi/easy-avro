package me.atour.easyavro;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class AvroSchemaTest {

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class AvroSchemaWrapper {
    private final Short wrapped;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class SimpleDto {
    private final Boolean boolOne;
    private final boolean boolTwo;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class SimpleWithStaticDto {
    private static String STRING;
    private final Boolean boolOne;
    private final boolean boolTwo;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class MultipleTypesDto {
    private final Boolean boolOne;
    private final char charOne;
    private final int intOne;
    private final Long longOne;
    private final Byte byteOne;
    private final double doubleOne;
    private final Float floatOne;
    private final short shortOne;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class MultipleOptionalTypesDto {
    private Boolean boolOne;
    private char charOne;
    private int intOne;
    private Long longOne;
    private Byte byteOne;
    private double doubleOne;
    private Float floatOne;
    private short shortOne;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class ArrayTypeDto {
    private final int[] intArrayOne;
    private final Integer[] intArrayTwo;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class MultipleArrayTypeDto {
    private final int[] intArray;
    private final double[] doubleArray;
    private final float[] floatArray;
    private final String[] stringArray;
    private final Character[] charArray;
    private final boolean[] boolArray;
    private final Long[] longArray;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class ListDto {
    private final List<String> stringList;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class MapDto {
    private final Map<String, Long> longMap;
  }

  @Test
  public void generateSchemaOfSimpleWrapper() {
    AvroSchema<AvroSchemaWrapper> schema = new AvroSchema<>(AvroSchemaWrapper.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_AvroSchemaWrapper")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredInt("wrapped")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaWithOnlyOnePrimitiveTypeTwoTimes() {
    AvroSchema<SimpleDto> schema = new AvroSchema<>(SimpleDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_SimpleDto")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredBoolean("boolTwo")
        .requiredBoolean("boolOne")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaWithTwoPrimitivesAndOneStaticField() {
    AvroSchema<SimpleWithStaticDto> schema = new AvroSchema<>(SimpleWithStaticDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_SimpleWithStaticDto")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredBoolean("boolOne")
        .requiredBoolean("boolTwo")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingMultiplePrimitiveTypes() {
    AvroSchema<MultipleOptionalTypesDto> schema = new AvroSchema<>(MultipleOptionalTypesDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_MultipleOptionalTypesDto")
        .namespace("me.atour.easyavro")
        .fields()
        .optionalInt("charOne")
        .optionalInt("shortOne")
        .optionalInt("byteOne")
        .optionalLong("longOne")
        .optionalFloat("floatOne")
        .optionalBoolean("boolOne")
        .optionalInt("intOne")
        .optionalDouble("doubleOne")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingMultipleFinalPrimitiveTypes() {
    AvroSchema<MultipleTypesDto> schema = new AvroSchema<>(MultipleTypesDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_MultipleTypesDto")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredBoolean("boolOne")
        .requiredInt("intOne")
        .requiredDouble("doubleOne")
        .requiredFloat("floatOne")
        .requiredLong("longOne")
        .requiredInt("byteOne")
        .requiredInt("shortOne")
        .requiredInt("charOne")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingArrayTypes() {
    AvroSchema<ArrayTypeDto> schema = new AvroSchema<>(ArrayTypeDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_ArrayTypeDto")
        .namespace("me.atour.easyavro")
        .fields()
        .name("intArrayTwo")
        .type()
        .array()
        .items()
        .intType()
        .noDefault()
        .name("intArrayOne")
        .type()
        .array()
        .items()
        .intType()
        .noDefault()
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingMultipleArrayTypes() {
    AvroSchema<MultipleArrayTypeDto> schema = new AvroSchema<>(MultipleArrayTypeDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_MultipleArrayTypeDto")
        .namespace("me.atour.easyavro")
        .fields()
        .name("longArray")
        .type()
        .array()
        .items()
        .longType()
        .noDefault()
        .name("floatArray")
        .type()
        .array()
        .items()
        .floatType()
        .noDefault()
        .name("boolArray")
        .type()
        .array()
        .items()
        .booleanType()
        .noDefault()
        .name("doubleArray")
        .type()
        .array()
        .items()
        .doubleType()
        .noDefault()
        .name("charArray")
        .type()
        .array()
        .items()
        .intType()
        .noDefault()
        .name("intArray")
        .type()
        .array()
        .items()
        .intType()
        .noDefault()
        .name("stringArray")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaWithOneList() {
    AvroSchema<ListDto> schema = new AvroSchema<>(ListDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_ListDto")
        .namespace("me.atour.easyavro")
        .fields()
        .name("stringList")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaWithOneMap() {
    AvroSchema<MapDto> schema = new AvroSchema<>(MapDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_MapDto")
        .namespace("me.atour.easyavro")
        .fields()
        .name("longMap")
        .type()
        .map()
        .values()
        .longType()
        .noDefault()
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void convertSimplePojoToGenericRecord() {
    AvroSchema<AvroSchemaWrapper> schema = new AvroSchema<>(AvroSchemaWrapper.class);
    schema.generate();
    AvroSchemaWrapper avroSchemaWrapper = new AvroSchemaWrapper((short) 1);
    GenericRecord expected = new GenericData.Record(schema.getSchema());
    expected.put("wrapped", (short) 1);
    assertThat(schema.convertFromPojo(avroSchemaWrapper)).isEqualTo(expected);
  }

  @Test
  public void convertSimplePojoWithStaticFieldToGenericRecord() {
    AvroSchema<SimpleWithStaticDto> schema = new AvroSchema<>(SimpleWithStaticDto.class);
    schema.generate();
    SimpleWithStaticDto dto = new SimpleWithStaticDto(false, true);
    GenericRecord expected = new GenericData.Record(schema.getSchema());
    expected.put("boolOne", false);
    expected.put("boolTwo", true);
    assertThat(schema.convertFromPojo(dto)).isEqualTo(expected);
  }

  @Test
  public void convertSimplePojoWithArrayFieldToGenericRecord() {
    AvroSchema<ArrayTypeDto> schema = new AvroSchema<>(ArrayTypeDto.class);
    schema.generate();
    ArrayTypeDto dto = new ArrayTypeDto(new int[] {1, 2}, new Integer[] {1, 2});
    GenericRecord actual = schema.convertFromPojo(dto);
    int[] actualArrayOne = (int[]) actual.get("intArrayOne");
    Integer[] actualArrayTwo = (Integer[]) actual.get("intArrayTwo");
    assertThat(actualArrayOne[0]).isEqualTo(1);
    assertThat(actualArrayOne[1]).isEqualTo(2);
    assertThat(actualArrayTwo[0]).isEqualTo(1);
    assertThat(actualArrayTwo[1]).isEqualTo(2);
  }
}
