package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to kebab case.
 */
public class KebabCaseNamingConverter implements FieldNamingConverter {

  @Override
  public String convert(@NonNull String name) {
    String replacement = "$1-$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toLowerCase();
  }
}