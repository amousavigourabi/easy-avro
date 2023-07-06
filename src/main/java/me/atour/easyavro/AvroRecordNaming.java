package me.atour.easyavro;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

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
