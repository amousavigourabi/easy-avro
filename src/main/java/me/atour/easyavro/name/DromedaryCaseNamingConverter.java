package me.atour.easyavro.name;

import lombok.NonNull;

public class DromedaryCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name;
  }
}
