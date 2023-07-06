package me.atour.easyavro;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AvroRecordNaming {

  enum FieldNamingStrategies {
    SNAKE_CASE,
    KEBAB_CASE,
    DROMEDARY_CASE,
    UPPERCASE,
    LOWERCASE,
    SCREAMING_SNAKE_CASE,
    PASCAL_CASE
  }

  String schemaName() default "";
  FieldNamingStrategies fieldStrategy() default FieldNamingStrategies.DROMEDARY_CASE;
}
