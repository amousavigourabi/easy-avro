package me.atour.easyavro;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import lombok.RequiredArgsConstructor;
import me.atour.easyavro.field.AvroField;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class AvroSchemaAnnotationTest {

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  @AvroRecord(schemaName = "wrapper", fieldStrategy = FieldNamingStrategies.SNAKE_CASE)
  private static class AvroSchemaWrapper {
    private final Short wrappedShort;
  }

  @SuppressWarnings("unused")
  @RequiredArgsConstructor
  @AvroRecord(schemaName = "optionals_dto", fieldStrategy = FieldNamingStrategies.SCREAMING_SNAKE_CASE)
  private static class SimpleDtoWithOptionals {
    private final Short shortOne;

    @AvroField(name = "boolean")
    private boolean boolOne;

    @AvroField(included = false)
    private String notIncluded;
  }

  @Test
  public void generateSchemaOfSimpleWrapper() {
    AvroSchema<AvroSchemaWrapper> schema = new AvroSchema<>(AvroSchemaWrapper.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("wrapper")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredInt("wrapped_short")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }

  @Test
  public void generateSchemaOfDtoWithFieldAnnotations() {
    AvroSchema<SimpleDtoWithOptionals> schema = new AvroSchema<>(SimpleDtoWithOptionals.class);
    schema.generate();
    Schema expected = SchemaBuilder.record("optionals_dto")
        .namespace("me.atour.easyavro")
        .fields()
        .requiredInt("SHORT_ONE")
        .optionalBoolean("boolean")
        .endRecord();
    assertThat(schema.getSchema()).isEqualTo(expected);
  }
}
