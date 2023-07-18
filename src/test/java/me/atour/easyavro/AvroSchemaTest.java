package me.atour.easyavro;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
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
  @EqualsAndHashCode
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
  @EqualsAndHashCode
  @RequiredArgsConstructor
  private static class MultiplePrimitiveTypesDto {
    private final boolean boolOne;
    private final char charOne;
    private final int intOne;
    private final long longOne;
    private final byte byteOne;
    private final double doubleOne;
    private final float floatOne;
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
  @EqualsAndHashCode
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
  private static class MultipleListTypeDto {
    private final List<Integer> intList;
    private final List<Double> doubleList;
    private final List<Float> floatList;
    private final List<String> stringList;
    private final List<Character> charList;
    private final List<Boolean> boolList;
    private final List<Long> longList;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class MapDto {
    private final Map<String, Long> longMap;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class MultipleMapTypeDto {
    private final Map<String, Integer> intMap;
    private final Map<String, Double> doubleMap;
    private final Map<String, Float> floatMap;
    private final Map<String, String> stringMap;
    private final Map<String, Character> charMap;
    private final Map<String, Boolean> boolMap;
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
        .requiredBoolean("bool_two")
        .requiredBoolean("bool_one")
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
        .requiredBoolean("bool_one")
        .requiredBoolean("bool_two")
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
        .optionalInt("char_one")
        .optionalInt("short_one")
        .optionalInt("byte_one")
        .optionalLong("long_one")
        .optionalFloat("float_one")
        .optionalBoolean("bool_one")
        .optionalInt("int_one")
        .optionalDouble("double_one")
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
        .requiredBoolean("bool_one")
        .requiredInt("int_one")
        .requiredDouble("double_one")
        .requiredFloat("float_one")
        .requiredLong("long_one")
        .requiredInt("byte_one")
        .requiredInt("short_one")
        .requiredInt("char_one")
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
        .name("int_array_two")
        .type()
        .array()
        .items()
        .intType()
        .arrayDefault(List.of())
        .name("int_array_one")
        .type()
        .array()
        .items()
        .intType()
        .arrayDefault(List.of())
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
        .name("long_array")
        .type()
        .array()
        .items()
        .longType()
        .arrayDefault(List.of())
        .name("float_array")
        .type()
        .array()
        .items()
        .floatType()
        .arrayDefault(List.of())
        .name("bool_array")
        .type()
        .array()
        .items()
        .booleanType()
        .arrayDefault(List.of())
        .name("double_array")
        .type()
        .array()
        .items()
        .doubleType()
        .arrayDefault(List.of())
        .name("char_array")
        .type()
        .array()
        .items()
        .intType()
        .arrayDefault(List.of())
        .name("int_array")
        .type()
        .array()
        .items()
        .intType()
        .arrayDefault(List.of())
        .name("string_array")
        .type()
        .array()
        .items()
        .stringType()
        .arrayDefault(List.of())
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
        .name("string_list")
        .type()
        .array()
        .items()
        .stringType()
        .arrayDefault(List.of())
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingMultipleListTypes() {
    AvroSchema<MultipleListTypeDto> schema = new AvroSchema<>(MultipleListTypeDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_MultipleListTypeDto")
        .namespace("me.atour.easyavro")
        .fields()
        .name("int_list")
        .type()
        .array()
        .items()
        .intType()
        .arrayDefault(List.of())
        .name("string_list")
        .type()
        .array()
        .items()
        .stringType()
        .arrayDefault(List.of())
        .name("float_list")
        .type()
        .array()
        .items()
        .floatType()
        .arrayDefault(List.of())
        .name("char_list")
        .type()
        .array()
        .items()
        .intType()
        .arrayDefault(List.of())
        .name("double_list")
        .type()
        .array()
        .items()
        .doubleType()
        .arrayDefault(List.of())
        .name("bool_list")
        .type()
        .array()
        .items()
        .booleanType()
        .arrayDefault(List.of())
        .name("long_list")
        .type()
        .array()
        .items()
        .longType()
        .arrayDefault(List.of())
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
        .name("long_map")
        .type()
        .map()
        .values()
        .longType()
        .mapDefault(Map.of())
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingMultipleMapTypes() {
    AvroSchema<MultipleMapTypeDto> schema = new AvroSchema<>(MultipleMapTypeDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("SchemaTest_MultipleMapTypeDto")
        .namespace("me.atour.easyavro")
        .fields()
        .name("char_map")
        .type()
        .map()
        .values()
        .intType()
        .mapDefault(Map.of())
        .name("double_map")
        .type()
        .map()
        .values()
        .doubleType()
        .mapDefault(Map.of())
        .name("bool_map")
        .type()
        .map()
        .values()
        .booleanType()
        .mapDefault(Map.of())
        .name("long_map")
        .type()
        .map()
        .values()
        .longType()
        .mapDefault(Map.of())
        .name("float_map")
        .type()
        .map()
        .values()
        .floatType()
        .mapDefault(Map.of())
        .name("int_map")
        .type()
        .map()
        .values()
        .intType()
        .mapDefault(Map.of())
        .name("string_map")
        .type()
        .map()
        .values()
        .stringType()
        .mapDefault(Map.of())
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
    expected.put("bool_one", false);
    expected.put("bool_two", true);
    assertThat(schema.convertFromPojo(dto)).isEqualTo(expected);
  }

  @Test
  public void convertSimplePojoWithArrayFieldToGenericRecord() {
    AvroSchema<ArrayTypeDto> schema = new AvroSchema<>(ArrayTypeDto.class);
    schema.generate();
    ArrayTypeDto dto = new ArrayTypeDto(new int[] {1, 2}, new Integer[] {1, 2});
    GenericRecord actual = schema.convertFromPojo(dto);
    int[] actualArrayOne = (int[]) actual.get("int_array_one");
    Integer[] actualArrayTwo = (Integer[]) actual.get("int_array_two");
    assertThat(actualArrayOne[0]).isEqualTo(1);
    assertThat(actualArrayOne[1]).isEqualTo(2);
    assertThat(actualArrayTwo[0]).isEqualTo(1);
    assertThat(actualArrayTwo[1]).isEqualTo(2);
  }

  @Test
  public void generatePojoFromRecordWithMultiplePrimitivesAndWrappers() {
    AvroSchema<MultipleTypesDto> schema = new AvroSchema<>(MultipleTypesDto.class);
    schema.generate();
    MultipleTypesDto dto = new MultipleTypesDto(false, 'a', 1, 2L, (byte) 3, 1.0, -0.7f, (short) 8);
    GenericRecord actual = schema.convertFromPojo(dto);
    MultipleTypesDto pojo = schema.convertToPojo(actual);
    assertThat(pojo).isEqualTo(dto);
  }

  @Test
  public void generatePojoFromRecordWithMultiplePrimitives() {
    AvroSchema<MultiplePrimitiveTypesDto> schema = new AvroSchema<>(MultiplePrimitiveTypesDto.class);
    schema.generate();
    MultiplePrimitiveTypesDto dto =
        new MultiplePrimitiveTypesDto(true, 'z', 81, -29191L, (byte) -10, 0.0001, 6.74f, (short) 0);
    GenericRecord actual = schema.convertFromPojo(dto);
    MultiplePrimitiveTypesDto pojo = schema.convertToPojo(actual);
    assertThat(pojo).isEqualTo(dto);
  }

  @Test
  public void generatePojoFromRecordWithArrays() {
    AvroSchema<ArrayTypeDto> schema = new AvroSchema<>(ArrayTypeDto.class);
    schema.generate();
    ArrayTypeDto dto = new ArrayTypeDto(new int[] {1, 2}, new Integer[] {1, 2});
    GenericRecord actual = schema.convertFromPojo(dto);
    ArrayTypeDto pojo = schema.convertToPojo(actual);
    assertThat(pojo).isEqualTo(dto);
  }
}
