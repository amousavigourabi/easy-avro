# Easy Avro

Simplify dynamic Avro schema generation and usage using POJOs.

Allows for easy definition of Avro schemas using POJOs.
Accompanied by a set of annotations to provide more control over the
generated schemas. After schema generation, mechanisms are provided to
convert to and from Avro records and POJOs for seamless usage.

Easy Avro supports all access levels, and both final and non-final fields alike.

## Usage

To use Easy Avro, you will need to interact with `me.atour.easyavro.AvroSchema`.
The `AvroSchema` class provides mechanisms to generate Avro schemas for provided POJOs and convert said POJOs to
Avro's `GenericRecords`, and back.

To get started with default settings, you can use it as follows.

```java
AvroSchema avroSchema = new AvroSchema(MyPojo.class);
avroSchema.generate();
GenericRecord record = avroSchema.convertFromPojo(pojoInstance);
MyPojo pojoInstance2 = avroSchema.convertToPojo(record);
```

In the snippet above, the first line defines the `AvroSchema` class, after which the second line generates
the Avro schema definition for the `AvroSchema`. The third line converts the POJO for which the schema was
generated to Avro's `GenericRecord`, after which line four converts the newly generated `GenericRecord` back
to a POJO.

To modify the standard behaviour of Easy Avro, you can use class- and field-level annotations. At class level,
`me.atour.easyavro.AvroRecord` would be used. This can be used to define the schema name and set the naming strategy
for class fields. By default, the snake case converter is used while the class name is used as the schema name,
after replacing `$` with `_` in the case of nested classes.

For the naming conversion strategy, six options are available: dromedary case, lowercase, pascal case,
screaming snake case, snake case, and uppercase. It is important to note that the field naming converters assume
the POJO's fields are already in dromedary case, as is customary in Java. As an example, the field `transportBuilder`
would be converted to `transportbuilder` in lowercase, `TransportBuilder` in pascal case, `TRANSPORT_BUILDER` in
screaming snake case, `transport_builder` in snake case, and `TRANSPORTBUILDER` in uppercase, while remaining as
`transportBuilder` in dromedary case. In the following snippet, the class-level `AvroRecord` annotation is used to
define both the schema name and naming conversion strategy, as `"SCHEMANAME"` and dromedary case respectively.

```java
import me.atour.easyavro.AvroRecord;
import me.atour.easyavro.FieldNamingStrategies;

@AvroRecord(schemaName = "SCHEMANAME", fieldStrategy = FieldNamingStrategies.DROMEDARY_CASE)
public class Pojo {
  private int[] x;
  private boolean y;
  private List<Integer> z;
}
```

It is important to note that the `sun.misc.Unsafe` class is used to convert Avro records to POJOs. This means
that the `Unsafe` will have to be available to the runtime environment for Easy Avro to function properly.

## Installation

To install the project, first clone it from GitHub. Then go to the
directory it was cloned to and run the Maven install command to install
the project to your local Maven repository.

```shell
mvn clean install
```

Then, you can use the project by including the following Maven
dependency in your projects.

```xml
<dependency>
  <groupId>me.atour</groupId>
  <artifactId>easy-avro</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Logging

For logging, SLF4J is used. Easy Avro does not provide an implementation, this means that it will use the no-op
logger implementation by default. If an implementation is used by the project that uses Easy Avro, Easy Avro will
use that logger instead.
