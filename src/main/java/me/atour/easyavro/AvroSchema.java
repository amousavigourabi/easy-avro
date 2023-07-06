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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.atour.easyavro.name.DromedaryCaseNamingConverter;
import me.atour.easyavro.name.FieldNamingConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Slf4j
@RequiredArgsConstructor
public class AvroSchema<T> {

  private final Class<T> clazz;
  private final Map<String, String> schemaFields = new HashMap<>();

  @Getter
  private Schema schema;

  public void generate() {
    schemaFields.clear();
    AvroRecordNaming namingAnnotation = clazz.getAnnotation(AvroRecordNaming.class);
    FieldNamingConverter fieldNameConverter;
    String schemaName;
    if (namingAnnotation == null) {
      fieldNameConverter = new DromedaryCaseNamingConverter();
      schemaName = clazz.getName();
    } else {
      fieldNameConverter = FieldNamingConverter.of(namingAnnotation.fieldStrategy());
      schemaName = namingAnnotation.schemaName().equals("") ? clazz.getName() : namingAnnotation.schemaName();
    }
    Field[] fields = clazz.getDeclaredFields();
    SchemaBuilder.FieldAssembler<Schema> schemaBuilder =
        SchemaBuilder.record(schemaName).namespace(clazz.getPackageName()).fields();
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
      for (Field field : nonStaticValidFields) {
        AvroField fieldAnnotation = field.getAnnotation(AvroField.class);
        if (fieldAnnotation != null && !fieldAnnotation.included()) {
          continue;
        }
        String originalFieldName = lookup.revealDirect(fieldHandles.get(field)).getName();
        String processedFieldName;
        if (fieldAnnotation == null || fieldAnnotation.name().equals("")) {
          processedFieldName = fieldNameConverter.convert(originalFieldName);
        } else {
          processedFieldName = fieldAnnotation.name();
        }
        VarHandle typeInfoHandle = lookup.unreflectVarHandle(field);
        Class<?> fieldType = typeInfoHandle.varType();
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
}
