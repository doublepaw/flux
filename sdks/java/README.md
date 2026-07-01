# Flux Java SDK

Java client library for Flux Event Bus.

## Installation

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>io.flux</groupId>
    <artifactId>flux-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

### FluxClient (High-Level API)

```java
import io.flux.sdk.*;
import io.flux.sdk.schema.*;

@FluxSchema(topic = "orders", namespace = "com.example")
public class OrderEvent {
    @NonNull public String orderId;
    public long amount;
}

// Connect
try (FluxClient client = FluxClient.connect(new ClientConfig().apiKey("tb_..."))) {
    // Send — one call handles schema registration + serialization + partitioning
    client.send(new OrderEvent("abc", 100));
    client.send(new OrderEvent("abc", 100), "abc".getBytes());  // key-based partition
    client.send(new OrderEvent("abc", 100), 2);                  // explicit partition
    client.sendToTopic(new OrderEvent("abc", 100), "orders-stg"); // topic override

    // Read — typed callback
    client.consume(OrderEvent.class, "my-group", event -> {
        System.out.println(event.orderId);
    });
}
```

### Writer (Low-Level API)

```java
import io.flux.sdk.*;
import io.flux.sdk.proto.BatchAck;
import io.flux.sdk.proto.Record;
import com.google.protobuf.ByteString;

// Connect
Writer writer = Writer.connect("ws://localhost:9000");

// Or with authentication
WriterConfig config = new WriterConfig()
    .url("ws://localhost:9000")
    .apiKey("tb_your_api_key");
Writer writer = Writer.connect(config);

// Send records
Record record = Record.newBuilder()
    .setKey(ByteString.copyFrom("key".getBytes()))
    .setValue(ByteString.copyFrom("value".getBytes()))
    .build();
BatchAck ack = writer.send(1, 0, 100, List.of(record));

System.out.println("Wrote records at offsets " + ack.getStartOffset() + " to " + ack.getEndOffset());

// Close
writer.close();
```

### Reader (with Reader Groups)

```java
import io.flux.sdk.*;
import io.flux.sdk.proto.PartitionResult;
import io.flux.sdk.proto.Record;

// Configure
ReaderConfig config = new ReaderConfig()
    .url("ws://localhost:9000")
    .groupId("my-group")
    .topicId(1);

// Join the group
GroupReader reader = GroupReader.join(config);

// Start heartbeat loop
reader.startHeartbeat();

// Poll for records
while (true) {
    List<PartitionResult> results = reader.poll();
    for (PartitionResult result : results) {
        for (Record record : result.getRecordsList()) {
            System.out.println("Received: " + new String(record.getValue().toByteArray()));
        }
    }

    // Commit offsets
    reader.commit();
}

// Stop
reader.stop();
reader.close();
```

### Schema

```java
import io.flux.sdk.schema.*;

// All fields are nullable by default. Use @NonNull to require a field.
@FluxSchema
public class Order {
    @NonNull public String orderId;
    public long amount;          // nullable ["null", "long"]
    public List<String> tags;    // nullable ["null", {"type":"array",...}]
}

// With evolution metadata
@FluxSchema(
    namespace = "com.example",
    renames = {@Rename(from = "old_name", to = "newName")},
    deletions = {"removedField"}
)
public class OrderV2 {
    @NonNull public String orderId;
    public int newName;  // gets aliases: ["old_name"]
}

// Generate schema JSON
String json = Schemas.schemaJson(Order.class);

// Serialize / deserialize (schemaless binary)
byte[] bytes = Schemas.toBytes(order);
Order restored = Schemas.fromBytes(Order.class, bytes);
```

Supported types: `String`, `int`/`Integer`, `long`/`Long`, `float`/`Float`, `double`/`Double`, `boolean`/`Boolean`, `byte[]`, `List<T>`, `Map<String,T>`, Java enums, nested `@FluxSchema` classes.

## Building

```bash
cd sdks/java/flux-sdk
mvn clean install
```

## Testing

```bash
mvn test
```

## Wire Protocol

The SDK uses generated protobuf messages from `proto/flux_wire.proto` for:
- outer WebSocket envelope (`ClientMessage`/`ServerMessage`)
- all request/response payloads
