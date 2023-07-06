package me.atour.easyavro.name;

import lombok.NonNull;

/**
 * Converts field names to uppercase.
 */
public class UppercaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name.toUpperCase();
  }
}
