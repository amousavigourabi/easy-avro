package me.atour.easyavro.field;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates fields to specify the name and inclusion in the {@link org.apache.avro.Schema} to generate.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AvroField {

  String name() default "";

  boolean included() default true;
}
