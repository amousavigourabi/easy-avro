package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to pascal case.
 */
public class PascalCaseNamingConverter implements FieldNamingConverter {

  /**
   * Converts the {@link String} to pascal case.
   *
   * @param name the field name to convert
   * @return the converted {@link String}
   */
  @Override
  public String convert(@NonNull String name) {
    if (name.length() < 1) {
      throw new IllegalArgumentException();
    }
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }
}
