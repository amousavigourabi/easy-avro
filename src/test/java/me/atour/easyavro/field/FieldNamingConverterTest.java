package me.atour.easyavro.field;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import me.atour.easyavro.AvroRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FieldNamingConverterTest {

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void getOf(AvroRecord.FieldNamingStrategies strategy) {
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    switch (strategy) {
      case LOWERCASE:
        assertThat(converter).isExactlyInstanceOf(LowercaseNamingConverter.class);
        break;
      case UPPERCASE:
        assertThat(converter).isExactlyInstanceOf(UppercaseNamingConverter.class);
        break;
      case KEBAB_CASE:
        assertThat(converter).isExactlyInstanceOf(KebabCaseNamingConverter.class);
        break;
      case SNAKE_CASE:
        assertThat(converter).isExactlyInstanceOf(SnakeCaseNamingConverter.class);
        break;
      case PASCAL_CASE:
        assertThat(converter).isExactlyInstanceOf(PascalCaseNamingConverter.class);
        break;
      case SCREAMING_SNAKE_CASE:
        assertThat(converter).isExactlyInstanceOf(ScreamingSnakeCaseNamingConverter.class);
        break;
      case DROMEDARY_CASE:
      default:
        assertThat(converter).isExactlyInstanceOf(DromedaryCaseNamingConverter.class);
    }
  }

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void convertLowercaseString(AvroRecord.FieldNamingStrategies strategy) {
    String original = "afullylowercasestring";
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    String result = converter.convert(original);
    switch (strategy) {
      case SNAKE_CASE:
      case LOWERCASE:
      case KEBAB_CASE:
      case DROMEDARY_CASE:
        assertThat(result).isEqualTo("afullylowercasestring");
        break;
      case UPPERCASE:
      case SCREAMING_SNAKE_CASE:
        assertThat(result).isEqualTo("AFULLYLOWERCASESTRING");
        break;
      case PASCAL_CASE:
      default:
        assertThat(result).isEqualTo("Afullylowercasestring");
    }
  }

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void convertUppercaseString(AvroRecord.FieldNamingStrategies strategy) {
    String original = "AFULLYUPPERCASESTRING";
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    String result = converter.convert(original);
    switch (strategy) {
      case LOWERCASE:
      case SNAKE_CASE:
      case KEBAB_CASE:
      default:
        assertThat(result).isEqualTo("afullyuppercasestring");
        break;
      case UPPERCASE:
      case PASCAL_CASE:
      case SCREAMING_SNAKE_CASE:
      case DROMEDARY_CASE:
        assertThat(result).isEqualTo("AFULLYUPPERCASESTRING");
    }
  }

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void convertStringWithSomeSingleDigitNumberInTheMiddle(AvroRecord.FieldNamingStrategies strategy) {
    String original = "aString9WithSomeNumber";
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    String result = converter.convert(original);
    switch (strategy) {
      case LOWERCASE:
        assertThat(result).isEqualTo("astring9withsomenumber");
        break;
      case UPPERCASE:
        assertThat(result).isEqualTo("ASTRING9WITHSOMENUMBER");
        break;
      case PASCAL_CASE:
        assertThat(result).isEqualTo("AString9WithSomeNumber");
        break;
      case SNAKE_CASE:
        assertThat(result).isEqualTo("a_string_9_with_some_number");
        break;
      case KEBAB_CASE:
        assertThat(result).isEqualTo("a-string-9-with-some-number");
        break;
      case SCREAMING_SNAKE_CASE:
        assertThat(result).isEqualTo("A_STRING_9_WITH_SOME_NUMBER");
        break;
      case DROMEDARY_CASE:
      default:
        assertThat(result).isEqualTo("aString9WithSomeNumber");
    }
  }

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void convertStringWithSomeSingleDigitNumberAtTheEnd(AvroRecord.FieldNamingStrategies strategy) {
    String original = "aStringWithSomeNumber9";
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    String result = converter.convert(original);
    switch (strategy) {
      case LOWERCASE:
        assertThat(result).isEqualTo("astringwithsomenumber9");
        break;
      case UPPERCASE:
        assertThat(result).isEqualTo("ASTRINGWITHSOMENUMBER9");
        break;
      case PASCAL_CASE:
        assertThat(result).isEqualTo("AStringWithSomeNumber9");
        break;
      case SNAKE_CASE:
        assertThat(result).isEqualTo("a_string_with_some_number_9");
        break;
      case KEBAB_CASE:
        assertThat(result).isEqualTo("a-string-with-some-number-9");
        break;
      case SCREAMING_SNAKE_CASE:
        assertThat(result).isEqualTo("A_STRING_WITH_SOME_NUMBER_9");
        break;
      case DROMEDARY_CASE:
      default:
        assertThat(result).isEqualTo("aStringWithSomeNumber9");
    }
  }

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void convertStringWithSomeSingleDigitNumberWithSuffixAtTheStart(AvroRecord.FieldNamingStrategies strategy) {
    String original = "2aStringWithSomeNumber";
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    String result = converter.convert(original);
    switch (strategy) {
      case LOWERCASE:
        assertThat(result).isEqualTo("2astringwithsomenumber");
        break;
      case UPPERCASE:
        assertThat(result).isEqualTo("2ASTRINGWITHSOMENUMBER");
        break;
      case PASCAL_CASE:
        assertThat(result).isEqualTo("2aStringWithSomeNumber");
        break;
      case SNAKE_CASE:
        assertThat(result).isEqualTo("2a_string_with_some_number");
        break;
      case KEBAB_CASE:
        assertThat(result).isEqualTo("2a-string-with-some-number");
        break;
      case SCREAMING_SNAKE_CASE:
        assertThat(result).isEqualTo("2A_STRING_WITH_SOME_NUMBER");
        break;
      case DROMEDARY_CASE:
      default:
        assertThat(result).isEqualTo("2aStringWithSomeNumber");
    }
  }

  @ParameterizedTest
  @EnumSource(AvroRecord.FieldNamingStrategies.class)
  public void convertStringWithUnderscores(AvroRecord.FieldNamingStrategies strategy) {
    String original = "a_string_with_underscores";
    FieldNamingConverter converter = FieldNamingConverter.of(strategy);
    String result = converter.convert(original);
    switch (strategy) {
      case LOWERCASE:
      case SNAKE_CASE:
      case KEBAB_CASE:
      case DROMEDARY_CASE:
        assertThat(result).isEqualTo("a_string_with_underscores");
        break;
      case PASCAL_CASE:
        assertThat(result).isEqualTo("A_string_with_underscores");
        break;
      case SCREAMING_SNAKE_CASE:
      case UPPERCASE:
      default:
        assertThat(result).isEqualTo("A_STRING_WITH_UNDERSCORES");
    }
  }
}
