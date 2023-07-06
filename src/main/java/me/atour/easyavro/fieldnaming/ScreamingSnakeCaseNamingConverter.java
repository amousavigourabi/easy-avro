package me.atour.easyavro.fieldnaming;

import lombok.NonNull;

public class ScreamingSnakeCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    String replacement = "$1_$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toUpperCase();
  }
}
