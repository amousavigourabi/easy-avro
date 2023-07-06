package me.atour.easyavro;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface AvroField {

  String name() default "";
  boolean included() default true;
}
