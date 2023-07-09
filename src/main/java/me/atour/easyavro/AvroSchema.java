package me.atour.easyavro;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.atour.easyavro.field.DromedaryCaseNamingConverter;
import me.atour.easyavro.field.FieldNamingConverter;
import org.apache.avro.Schema;
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

  private final Class<T> clazz;
  private final Map<String, String> schemaFields = new ConcurrentHashMap<>();

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
      String schemaNamespace = clazz.getPackageName();
      SchemaFactory schemaFactory = new SchemaFactory(schemaName, schemaNamespace);
      Map<String, String> nameMap = schemaFactory.setFields(fieldHandles, lookup, fieldNameConverter);
      schema = schemaFactory.create();
      schemaFields.clear();
      schemaFields.putAll(nameMap);
    } catch (IllegalAccessException e) {
      log.error("Cannot generate a valid schema in {} because {}.", clazz, e.getMessage());
      throw new CannotGenerateSchemaException(e);
    }
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
}
