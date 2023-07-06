package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to lowercase.
 */
public class LowercaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name.toLowerCase();
  }
}
