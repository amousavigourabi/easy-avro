package me.atour.easyavro.field;

import lombok.NonNull;

/**
 * Converts field names to screaming snake case.
 */
public class ScreamingSnakeCaseNamingConverter implements FieldNamingConverter {

  /**
   * Converts the {@link String} to screaming snake case.
   *
   * @param name the field name to convert
   * @return the converted {@link String}
   */
  @Override
  public String convert(@NonNull String name) {
    String replacement = "$1_$2";
    return name.replaceAll(followedByCapitalized, replacement)
        .replaceAll(followedByDigit, replacement)
        .toUpperCase();
  }
}
