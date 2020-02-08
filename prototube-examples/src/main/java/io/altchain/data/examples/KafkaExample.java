package io.altchain.data.examples;

import com.google.protobuf.ByteString;
import io.altchain.data.prototube.kafka.Producer;
import java.util.Random;
import java.util.UUID;


public class KafkaExample {

  private static Random RANDOM = new Random(System.currentTimeMillis());

  public static void main(String[] args) {
    Producer kafkaProducer = new Producer("prototube_demo", "localhost:9092");
    for (int i = 0; i< 100; i++) {
      kafkaProducer.emit(generateRandomEvent());
    }
    kafkaProducer.close();
  }

  private static Example.ExamplePrototubeMessage generateRandomEvent() {
    return Example.ExamplePrototubeMessage.newBuilder().setInt32Field(RANDOM.nextInt()).setInt64Field(RANDOM.nextLong())
        .setDoubleField(RANDOM.nextDouble()).setStringField(UUID.randomUUID().toString())
        .setBytesField(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build();
  }
}
