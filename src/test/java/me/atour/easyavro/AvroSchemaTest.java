package me.atour.easyavro;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class AvroSchemaTest {

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  private static class Wrapper {
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

  @Test
  public void generateSchemaOfSimpleWrapper() {
    AvroSchema<Wrapper> schema = new AvroSchema<>(Wrapper.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("AvroSchemaTest_Wrapper")
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
    Schema expected = SchemaBuilder.record("AvroSchemaTest_SimpleDto")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredBoolean("boolTwo")
        .requiredBoolean("boolOne")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaContainingMultiplePrimitiveTypes() {
    AvroSchema<MultipleOptionalTypesDto> schema = new AvroSchema<>(MultipleOptionalTypesDto.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("AvroSchemaTest_MultipleOptionalTypesDto")
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
    Schema expected = SchemaBuilder.record("AvroSchemaTest_MultipleTypesDto")
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
  public void convertSimplePojoToGenericRecord() {
    AvroSchema<Wrapper> schema = new AvroSchema<>(Wrapper.class);
    schema.generate();
    Wrapper wrapper = new Wrapper((short) 1);
    GenericRecord expected = new GenericData.Record(schema.getSchema());
    expected.put("wrapped", (short) 1);
    assertThat(schema.convertFromPojo(wrapper)).isEqualTo(expected);
  }
}
