package me.atour.easyavro.fieldnaming;

import lombok.NonNull;

public class UppercaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name.toUpperCase();
  }
}
