package me.atour.easyavro.name;

import lombok.NonNull;
import me.atour.easyavro.AvroRecordNaming;

/**
 * Interface for field name converters.
 * Extended for all the supported field naming strategies.
 */
public interface FieldNamingConverter {

  String followedByCapitalized = "([a-z0-9])([A-Z]+)";
  String followedByDigit = "([a-zA-Z])([0-9]+)";

  /**
   * Converts the {@link String} in accordance with the chosen naming strategy.
   *
   * @param name the field name to convert
   * @return the converted {@link String}
   */
  String convert(@NonNull String name);

  /**
   * Provides a {@link FieldNamingConverter} for the given strategy.
   *
   * @param strategy the strategy to return the corresponding {@link FieldNamingConverter} for
   * @return the corresponding {@link FieldNamingConverter}
   */
  static FieldNamingConverter of(@NonNull AvroRecordNaming.FieldNamingStrategies strategy) {
    FieldNamingConverter converter;
    switch (strategy) {
      case LOWERCASE:
        converter = new LowercaseNamingConverter();
        break;
      case UPPERCASE:
        converter = new UppercaseNamingConverter();
        break;
      case KEBAB_CASE:
        converter = new KebabCaseNamingConverter();
        break;
      case SNAKE_CASE:
        converter = new SnakeCaseNamingConverter();
        break;
      case PASCAL_CASE:
        converter = new PascalCaseNamingConverter();
        break;
      case SCREAMING_SNAKE_CASE:
        converter = new ScreamingSnakeCaseNamingConverter();
        break;
      case DROMEDARY_CASE:
      default:
        converter = new DromedaryCaseNamingConverter();
        break;
    }
    return converter;
  }
}
