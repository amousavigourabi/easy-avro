package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to dromedary case.
 */
public class DromedaryCaseNamingConverter implements FieldNamingConverter {

  /**
   * Converts the {@link String} to dromedary case.
   *
   * @param name the field name to convert
   * @return the converted {@link String}
   */
  @Override
  public String convert(@NonNull String name) {
    return name;
  }
}
