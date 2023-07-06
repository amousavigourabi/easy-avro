package me.atour.easyavro.fieldnaming;

import lombok.NonNull;
import me.atour.easyavro.AvroRecordNaming;

public interface FieldNamingConverter {

  String followedByCapitalized = "([a-z0-9])([A-Z]+)";
  String followedByDigit = "([a-zA-Z])([0-9]+)";

  String convert(@NonNull String name);

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
