package me.atour.easyavro.name;

import lombok.NonNull;

public class LowercaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    return name.toLowerCase();
  }
}
