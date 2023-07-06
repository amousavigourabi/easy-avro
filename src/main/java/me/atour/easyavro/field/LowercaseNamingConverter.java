package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to lowercase.
 */
public class LowercaseNamingConverter implements FieldNamingConverter {

  /**
   * Converts the {@link String} to lowercase.
   *
   * @param name the field name to convert
   * @return the converted {@link String}
   */
  @Override
  public String convert(@NonNull String name) {
    return name.toLowerCase();
  }
}
