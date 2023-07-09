package me.atour.easyavro;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

class ExceptionTest {

  @Test
  public void cannotGenerateSchemaExceptionIsRuntimeException() {
    assertThat(CannotGenerateSchemaException.class).isAssignableTo(RuntimeException.class);
  }

  @Test
  public void cannotCreateValidEncodingExceptionIsRuntimeException() {
    assertThat(CannotCreateValidEncodingException.class).isAssignableTo(RuntimeException.class);
  }
}
