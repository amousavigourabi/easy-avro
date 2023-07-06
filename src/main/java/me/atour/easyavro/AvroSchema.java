package me.atour.easyavro;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Slf4j
@RequiredArgsConstructor
public class AvroSchema<T> {

  String followedByCapitalized = "([a-z0-9])([A-Z]+)";
  String followedByDigit = "([a-zA-Z])([0-9]+)";

  private final Class<T> clazz;
  private final Map<String, String> schemaFields = new HashMap<>();

  @Getter
  private Schema schema;

  public void generate() {
    schemaFields.clear();
    Field[] fields = clazz.getDeclaredFields();
    SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(clazz.getName()).fields();
    try {
      List<Field> nonStaticValidFields = new ArrayList<>();
      Map<Field, MethodHandle> fieldHandles = new HashMap<>();
      MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
      for (Field field : fields) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        nonStaticValidFields.add(field);
        fieldHandles.put(field, lookup.unreflectGetter(field));
      }
      AvroRecordNaming namingAnnotation = clazz.getAnnotation(AvroRecordNaming.class);
      UnaryOperator<String> fieldNameConverter;
      if (namingAnnotation != null) {
        switch (namingAnnotation.strategy()) {
          case SCREAMING_SNAKE_CASE:
            fieldNameConverter = this::toScreamingSnakeCase;
            break;
          case KEBAB_CASE:
            fieldNameConverter = this::toKebabCase;
            break;
          case LOWERCASE:
            fieldNameConverter = this::toLowerCase;
            break;
          case UPPERCASE:
            fieldNameConverter = this::toUpperCase;
            break;
          case PASCAL_CASE:
            fieldNameConverter = this::toPascalCase;
            break;
          case SNAKE_CASE:
            fieldNameConverter = this::toSnakeCase;
            break;
          case DROMEDARY_CASE:
          default:
            fieldNameConverter = this::toDromedaryCase;
            break;
        }
      } else {
        fieldNameConverter = this::toDromedaryCase;
      }
      for (Field field : nonStaticValidFields) {
        VarHandle typeInfoHandle = lookup.unreflectVarHandle(field);
        Class<?> fieldType = typeInfoHandle.varType();
        String originalFieldName = lookup.revealDirect(fieldHandles.get(field)).getName();
        String processedFieldName = fieldNameConverter.apply(originalFieldName);
        if (String.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredString(processedFieldName);
        } else if (long.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredLong(processedFieldName);
        } else if (int.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredInt(processedFieldName);
        } else if (double.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredDouble(processedFieldName);
        } else if (long[].class.isAssignableFrom(fieldType)
            || Long[].class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.name(processedFieldName).type().array().items().longType().noDefault();
        } else if (Map.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.name(processedFieldName).type().map().values().stringType().noDefault();
        } else {
          log.error("Cannot create a valid encoding for {}.", fieldType);
          throw new IllegalAccessException(fieldType.toString());
        }
        schemaFields.put(originalFieldName, processedFieldName);
      }
      schema = schemaBuilder.endRecord();
    } catch (Throwable e) {
      log.error("Cannot generate a valid schema for {} in {}.", e.getMessage(), clazz);
      throw new CannotGenerateSchemaException(e);
    }
  }

  public GenericRecord convertFromPojo(T pojo) {
    Field[] fields = clazz.getDeclaredFields();
    GenericData.Record record = new GenericData.Record(schema);
    try {
      for (Field field : fields) {
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
        MethodHandle handle = lookup.unreflectGetter(field);
        if (!Modifier.isStatic(field.getModifiers()) && schemaFields.containsKey(lookup.revealDirect(handle).getName())) {
          record.put(schemaFields.get(lookup.revealDirect(handle).getName()), handle.invoke(pojo));
        }
      }
    } catch (Throwable e) {
      log.error("Could not convert to Avro record {}.", e.getMessage());
    }
    return record;
  }

  public String toSnakeCase(@NonNull String name) {
    String replacement = "$1_$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toLowerCase();
  }

  public String toKebabCase(@NonNull String name) {
    String replacement = "$1-$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toLowerCase();
  }

  public String toScreamingSnakeCase(@NonNull String name) {
    String replacement = "$1-$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toUpperCase();
  }

  public String toLowerCase(@NonNull String name) {
    return name.toLowerCase();
  }

  public String toUpperCase(@NonNull String name) {
    return name.toUpperCase();
  }

  public String toPascalCase(@NonNull String name) {
    if (name.length() < 1) {
      throw new IllegalArgumentException();
    }
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  public String toDromedaryCase(@NonNull String name) {
    return name;
  }
}
