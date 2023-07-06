package me.atour.easyavro.fieldnaming;

import lombok.NonNull;

public class PascalCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    if (name.length() < 1) {
      throw new IllegalArgumentException();
    }
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }
}
