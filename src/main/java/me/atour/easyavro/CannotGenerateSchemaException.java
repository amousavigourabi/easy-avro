package me.atour.easyavro;

/**
 * Thrown when a {@link org.apache.avro.Schema} cannot be generated for the provided {@link Class}.
 */
public class CannotGenerateSchemaException extends RuntimeException {

  /**
   * Constructs the exception with the {@link Throwable} that caused it.
   *
   * @param cause the cause of this exception being thrown
   */
  public CannotGenerateSchemaException(Throwable cause) {
    super(cause);
  }
}
