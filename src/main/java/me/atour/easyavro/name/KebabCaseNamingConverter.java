package me.atour.easyavro.name;

import lombok.NonNull;

public class KebabCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    String replacement = "$1-$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toLowerCase();
  }
}
