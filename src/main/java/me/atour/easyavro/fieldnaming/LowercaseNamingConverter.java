package me.atour.easyavro.fieldnaming;

import lombok.NonNull;

public class LowercaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name.toLowerCase();
  }
}
