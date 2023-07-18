package me.atour.easyavro;

/**
 * Thrown when a POJO cannot be generated from the provided {@link org.apache.avro.generic.GenericRecord}.
 */
public class CannotConvertRecordToPojoException extends RuntimeException {

  /**
   * Constructs the exception with the {@link Throwable} that caused it.
   *
   * @param cause the cause of this exception being thrown
   */
  public CannotConvertRecordToPojoException(Throwable cause) {
    super(cause);
  }
}
