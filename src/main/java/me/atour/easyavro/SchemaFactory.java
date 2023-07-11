package me.atour.easyavro;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import me.atour.easyavro.field.AvroField;
import me.atour.easyavro.field.FieldNamingConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

@Slf4j
public class SchemaFactory {

  private static final Map<Class<?>, Class<?>> wrapperMap = Map.of(
      boolean.class, Boolean.class,
      byte.class, Byte.class,
      char.class, Character.class,
      double.class, Double.class,
      float.class, Float.class,
      int.class, Integer.class,
      long.class, Long.class,
      short.class, Short.class);

  private static final Map<Class<?>, Class<?>> simplifyMap = Map.of(
      Byte.class, Integer.class,
      Character.class, Integer.class,
      Short.class, Integer.class);

  private SchemaBuilder.FieldAssembler<Schema> builder;

  /**
   * Constructs a {@link Schema} factory.
   *
   * @param name the name of the {@link Schema} to configure
   * @param namespace the namespace of the {@link Schema}
   */
  public SchemaFactory(@NonNull String name, @NonNull String namespace) {
    builder = SchemaBuilder.record(name).namespace(namespace).fields();
  }

  /**
   * Adds the fields to the {@link Schema} the factory is building.
   *
   * @param fields a {@link Map} of the {@link Field}s to add to their respective {@link MethodHandle}rs
   * @param lookup the {@link MethodHandles.Lookup} to use for reflection
   * @param fieldNameConverter the {@link FieldNamingConverter} of the strategy to apply on the fields
   * @return a {@link Map} detailing which fields were assigned which names in the schema
   * @throws IllegalAccessException when reflection cannot access the fields it tries to access
   */
  public Map<String, String> setFields(
      Map<Field, MethodHandle> fields, MethodHandles.Lookup lookup, FieldNamingConverter fieldNameConverter)
      throws IllegalAccessException {
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
      setField(fieldType, processedFieldName, isFinal);
      fieldNames.put(originalFieldName, processedFieldName);
    }
    return fieldNames;
  }

  /**
   * Adds a field to the {@link Schema} the factory is building.
   *
   * @param fieldType the field type as a Java {@link Class}
   * @param fieldName the field name
   * @param isRequired whether the field is required
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  private void setField(Class<?> fieldType, String fieldName, boolean isRequired) {
    if (fieldType.isArray()) {
      setArrayField(fieldType.getComponentType(), fieldName);
    } else if (isRequired) {
      setRequiredField(fieldType, fieldName);
    } else {
      setOptionalField(fieldType, fieldName);
    }
  }

  /**
   * Adds a required field to the {@link Schema} the factory is building.
   *
   * @param fieldType the field type as a Java {@link Class}
   * @param fieldName the field name
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  private void setRequiredField(Class<?> fieldType, String fieldName) {
    Class<?> wrappedType = simplifyType(fieldType);
    if (Boolean.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredBoolean(fieldName);
    } else if (Long.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredLong(fieldName);
    } else if (Integer.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredInt(fieldName);
    } else if (Double.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredDouble(fieldName);
    } else if (Float.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredFloat(fieldName);
    } else if (String.class.isAssignableFrom(wrappedType)) {
      builder = builder.requiredString(fieldName);
    } else if (wrappedType.isAnnotationPresent(AvroRecord.class)) {
      AvroSchema<?> schema = new AvroSchema<>(wrappedType);
      schema.generate();
      builder = builder.name(fieldName).type(schema.getSchema()).noDefault();
    } else {
      log.error("Cannot create a valid encoding for {}.", fieldType);
      throw new CannotCreateValidEncodingException();
    }
  }

  /**
   * Adds an optional field to the {@link Schema} the factory is building.
   *
   * @param fieldType the field type as a Java {@link Class}
   * @param fieldName the field name
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  private void setOptionalField(Class<?> fieldType, String fieldName) {
    Class<?> wrappedType = simplifyType(fieldType);
    if (Boolean.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalBoolean(fieldName);
    } else if (Long.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalLong(fieldName);
    } else if (Integer.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalInt(fieldName);
    } else if (Double.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalDouble(fieldName);
    } else if (Float.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalFloat(fieldName);
    } else if (String.class.isAssignableFrom(wrappedType)) {
      builder = builder.optionalString(fieldName);
    } else if (wrappedType.isAnnotationPresent(AvroRecord.class)) {
      AvroSchema<?> schema = new AvroSchema<>(wrappedType);
      schema.generate();
      builder = builder.name(fieldName).type(schema.getSchema()).noDefault();
    }
  }

  /**
   * Adds an array field to the {@link Schema} the factory is building.
   *
   * @param fieldType the component type of the field type as a Java {@link Class}
   * @param fieldName the field name
   * @throws CannotCreateValidEncodingException when the {@link Class} cannot be encoded
   */
  private void setArrayField(Class<?> fieldType, String fieldName) {
    Class<?> wrappedType = simplifyType(fieldType);
    if (Boolean.class.isAssignableFrom(wrappedType)) {
      builder =
          builder.name(fieldName).type().array().items().booleanType().noDefault();
    } else if (Long.class.isAssignableFrom(wrappedType)) {
      builder = builder.name(fieldName).type().array().items().longType().noDefault();
    } else if (Integer.class.isAssignableFrom(wrappedType)) {
      builder = builder.name(fieldName).type().array().items().intType().noDefault();
    } else if (Double.class.isAssignableFrom(wrappedType)) {
      builder =
          builder.name(fieldName).type().array().items().doubleType().noDefault();
    } else if (Float.class.isAssignableFrom(wrappedType)) {
      builder = builder.name(fieldName).type().array().items().floatType().noDefault();
    } else if (String.class.isAssignableFrom(wrappedType)) {
      builder =
          builder.name(fieldName).type().array().items().stringType().noDefault();
    } else if (wrappedType.isAnnotationPresent(AvroRecord.class)) {
      AvroSchema<?> schema = new AvroSchema<>(wrappedType);
      schema.generate();
      builder = builder.name(fieldName)
          .type()
          .array()
          .items()
          .type(schema.getSchema())
          .noDefault();
    } else {
      log.error("Cannot create a valid encoding for {}.", fieldType);
      throw new CannotCreateValidEncodingException();
    }
  }

  /**
   * Transforms a {@link Class} representing a primitive type to corresponding its wrapper.
   * Simplifies some classes further, {@link Short} to {@link Integer} for example.
   *
   * @param toSimplify the {@link Class} to transform
   * @return a simplified {@link Class} representation
   */
  private Class<?> simplifyType(@NonNull Class<?> toSimplify) {
    Class<?> wrapper = wrapperMap.getOrDefault(toSimplify, toSimplify);
    if (!simplifyMap.containsKey(wrapper)) {
      return wrapper;
    }
    return simplifyMap.get(wrapper);
  }

  /**
   * Creates the {@link Schema} configured for this factory.
   *
   * @return the configured {@link Schema}
   */
  public Schema create() {
    return builder.endRecord();
  }
}
