package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to uppercase.
 */
public class UppercaseNamingConverter implements FieldNamingConverter {

  /**
   * Converts the {@link String} to uppercase.
   *
   * @param name the field name to convert
   * @return the converted {@link String}
   */
  @Override
  public String convert(@NonNull String name) {
    return name.toUpperCase();
  }
}
