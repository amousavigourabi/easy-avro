package me.atour.easyavro;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.atour.easyavro.field.AvroField;
import me.atour.easyavro.field.DromedaryCaseNamingConverter;
import me.atour.easyavro.field.FieldNamingConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Generates the {@link Schema} and corresponding {@link GenericRecord}s from POJOs.
 *
 * @param <T> type parameter representing the class the POJOs belong to and the {@link Schema} is generated for.
 */
@Slf4j
@RequiredArgsConstructor
public class AvroSchema<T> {

  private static final Map<Class<?>, Class<?>> wrapperMap = Map.of(
      boolean.class, Boolean.class,
      byte.class, Byte.class,
      char.class, Character.class,
      double.class, Double.class,
      float.class, Float.class,
      int.class, Integer.class,
      long.class, Long.class,
      short.class, Short.class);

  private final Class<T> clazz;
  private final Map<String, String> schemaFields = new HashMap<>();

  @Getter
  private Schema schema;

  /**
   * Generates the schema belonging to the {@link Class} this {@link AvroSchema} was instantiated with.
   */
  public void generate() {
    AvroRecord namingAnnotation = clazz.getAnnotation(AvroRecord.class);
    FieldNamingConverter fieldNameConverter;
    String schemaName;
    if (namingAnnotation == null) {
      fieldNameConverter = new DromedaryCaseNamingConverter();
      schemaName = clazz.getName();
    } else {
      fieldNameConverter = FieldNamingConverter.of(namingAnnotation.fieldStrategy());
      schemaName = namingAnnotation.schemaName().equals("") ? clazz.getName() : namingAnnotation.schemaName();
    }
    try {
      Map<Field, MethodHandle> fieldHandles = new HashMap<>();
      MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
      for (Field field : clazz.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        fieldHandles.put(field, lookup.unreflectGetter(field));
      }
      SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(schemaName)
          .namespace(clazz.getPackageName())
          .fields();
      setFields(fieldHandles, lookup, fieldNameConverter, schemaBuilder);
    } catch (IllegalAccessException e) {
      log.error("Cannot generate a valid schema in {} because {}.", clazz, e.getMessage());
      throw new CannotGenerateSchemaException(e);
    }
  }

  /**
   * Sets the passed fields in the schema.
   *
   * @param fields a {@link Map} of the {@link Field}s to add to their respective {@link MethodHandle}rs
   * @param lookup the {@link MethodHandles.Lookup} to use for reflection
   * @param fieldNameConverter the {@link FieldNamingConverter} of the strategy to apply on the fields
   * @param schemaBuilder the {@link SchemaBuilder} to add the fields to
   * @throws IllegalAccessException when reflection cannot access the fields it tries to access
   */
  public void setFields(
      Map<Field, MethodHandle> fields,
      MethodHandles.Lookup lookup,
      FieldNamingConverter fieldNameConverter,
      SchemaBuilder.FieldAssembler<Schema> schemaBuilder)
      throws IllegalAccessException {
    SchemaBuilder.FieldAssembler<Schema> builder = schemaBuilder;
    Map<String, String> fieldNames = new HashMap<>();
    for (Map.Entry<Field, MethodHandle> field : fields.entrySet()) {
      AvroField fieldAnnotation = field.getKey().getAnnotation(AvroField.class);
      if (fieldAnnotation != null && !fieldAnnotation.included()) {
        continue;
      }
      String originalFieldName = lookup.revealDirect(field.getValue()).getName();
      String processedFieldName;
      if (fieldAnnotation == null || fieldAnnotation.name().equals("")) {
        processedFieldName = fieldNameConverter.convert(originalFieldName);
      } else {
        processedFieldName = fieldAnnotation.name();
      }
      VarHandle typeInfoHandle = lookup.unreflectVarHandle(field.getKey());
      Class<?> fieldType = typeInfoHandle.varType();
      boolean isFinal = Modifier.isFinal(field.getKey().getModifiers());
      builder = setField(fieldType, processedFieldName, schemaBuilder, isFinal);
      fieldNames.put(originalFieldName, processedFieldName);
    }
    schemaFields.clear();
    schemaFields.putAll(fieldNames);
    schema = builder.endRecord();
  }

  /**
   * Converts a POJO to a {@link GenericRecord}.
   *
   * @param pojo the POJO to convert to a {@link GenericRecord}
   * @return the generated {@link GenericRecord}
   */
  public GenericRecord convertFromPojo(T pojo) {
    Field[] fields = clazz.getDeclaredFields();
    GenericData.Record record = new GenericData.Record(schema);
    try {
      for (Field field : fields) {
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
        MethodHandle handle = lookup.unreflectGetter(field);
        if (!Modifier.isStatic(field.getModifiers())
            && schemaFields.containsKey(lookup.revealDirect(handle).getName())) {
          record.put(schemaFields.get(lookup.revealDirect(handle).getName()), handle.invoke(pojo));
        }
      }
    } catch (Throwable e) {
      log.error("Could not convert to Avro record {}.", e.getMessage());
    }
    return record;
  }

  /**
   * Transforms a {@link Class} representing a primitive type to corresponding its wrapper.
   *
   * @param possiblyPrimitive the {@link Class} to transform
   * @return a wrapped {@link Class} representation if the provided {@link Class} is a primitive
   */
  public Class<?> toWrapper(@NonNull Class<?> possiblyPrimitive) {
    if (!possiblyPrimitive.isPrimitive()) {
      return possiblyPrimitive;
    }
    return wrapperMap.get(possiblyPrimitive);
  }

  /**
   * Adds a field to the {@link SchemaBuilder}.
   *
   * @param fieldType the field type as a Java {@link Class}
   * @param fieldName the field name
   * @param schemaBuilder the {@link SchemaBuilder.FieldAssembler} to add the field to
   * @param isRequired whether the field is required
   * @return the {@link SchemaBuilder.FieldAssembler} with the field added
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  public SchemaBuilder.FieldAssembler<Schema> setField(
      Class<?> fieldType,
      String fieldName,
      @NonNull SchemaBuilder.FieldAssembler<Schema> schemaBuilder,
      boolean isRequired) {
    if (isRequired) {
      return setRequiredField(fieldType, fieldName, schemaBuilder);
    } else {
      return setOptionalField(fieldType, fieldName, schemaBuilder);
    }
  }

  /**
   * Adds a required field to the {@link SchemaBuilder}.
   *
   * @param fieldType the field type as a Java {@link Class}
   * @param fieldName the field name
   * @param schemaBuilder the {@link SchemaBuilder.FieldAssembler} to add the field to
   * @return the {@link SchemaBuilder.FieldAssembler} with the field added
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  public SchemaBuilder.FieldAssembler<Schema> setRequiredField(
      Class<?> fieldType, String fieldName, @NonNull SchemaBuilder.FieldAssembler<Schema> schemaBuilder) {
    SchemaBuilder.FieldAssembler<Schema> builder = schemaBuilder;
    Class<?> wrappedType = toWrapper(fieldType);
    if (Boolean.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredBoolean(fieldName);
    } else if (Long.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredLong(fieldName);
    } else if (Integer.class.isAssignableFrom(wrappedType)
        || Byte.class.isAssignableFrom(wrappedType)
        || Character.class.isAssignableFrom(wrappedType)
        || Short.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredInt(fieldName);
    } else if (Double.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredDouble(fieldName);
    } else if (Float.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredFloat(fieldName);
    } else {
      log.error("Cannot create a valid encoding for {}.", fieldType);
      throw new CannotCreateValidEncodingException();
    }
    return builder;
  }

  /**
   * Adds an optional field to the {@link SchemaBuilder}.
   *
   * @param fieldType the field type as a Java {@link Class}
   * @param fieldName the field name
   * @param schemaBuilder the {@link SchemaBuilder.FieldAssembler} to add the field to
   * @return the {@link SchemaBuilder.FieldAssembler} with the field added
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  public SchemaBuilder.FieldAssembler<Schema> setOptionalField(
      Class<?> fieldType, String fieldName, @NonNull SchemaBuilder.FieldAssembler<Schema> schemaBuilder) {
    SchemaBuilder.FieldAssembler<Schema> builder = schemaBuilder;
    Class<?> wrappedType = toWrapper(fieldType);
    if (Boolean.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalBoolean(fieldName);
    } else if (Long.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalLong(fieldName);
    } else if (Integer.class.isAssignableFrom(wrappedType)
        || Byte.class.isAssignableFrom(wrappedType)
        || Character.class.isAssignableFrom(wrappedType)
        || Short.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalInt(fieldName);
    } else if (Double.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalDouble(fieldName);
    } else if (Float.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalFloat(fieldName);
    } else {
      log.error("Cannot create a valid encoding for {}.", fieldType);
      throw new CannotCreateValidEncodingException();
    }
    return builder;
  }
}
