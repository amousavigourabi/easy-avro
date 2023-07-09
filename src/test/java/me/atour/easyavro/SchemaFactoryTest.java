package me.atour.easyavro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaFactoryTest {

  @ParameterizedTest
  @MethodSource("provideNameAndNamespaceParameters")
  public void instantiateWithNull(String name, String namespace, boolean npe) {
    if (npe) {
      assertThatThrownBy(() -> new SchemaFactory(name, namespace))
          .isExactlyInstanceOf(NullPointerException.class);
    } else {
      assertThat(new SchemaFactory(name, namespace)).isNotNull();
    }
  }

  private static Stream<Arguments> provideNameAndNamespaceParameters() {
    return Stream.of(
        Arguments.of("customer", null, true),
        Arguments.of(null, "com.example", true),
        Arguments.of(null, null, true),
        Arguments.of("customer", "com.example", false));
  }
}
