package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to pascal case.
 */
public class PascalCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    if (name.length() < 1) {
      throw new IllegalArgumentException();
    }
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }
}
