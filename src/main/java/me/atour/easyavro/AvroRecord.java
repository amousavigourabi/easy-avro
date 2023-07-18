package me.atour.easyavro;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies class-wide {@link org.apache.avro.Schema} naming properties.
 * This includes the field naming strategy (lowercase, camel case, snake case, etc.) and the schema name.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AvroRecord {

  String schemaName() default "";

  FieldNamingStrategies fieldStrategy() default FieldNamingStrategies.SNAKE_CASE;
}
