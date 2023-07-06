package me.atour.easyavro.name;

import lombok.NonNull;

/**
 * Converts field names to dromedary case.
 */
public class DromedaryCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name;
  }
}
