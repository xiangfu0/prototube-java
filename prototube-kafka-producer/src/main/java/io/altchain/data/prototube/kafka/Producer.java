package io.altchain.data.prototube.kafka;

import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class Producer implements AutoCloseable {
  private static final int DEFAULT_RETRY_BACKOFF_MS = 1000 * 10;
  private static final int DEFAULT_BATCH_SIZE_CONFIG = 1024 * 1024;
  private static final int SECONDS_TO_MILLS = 1000;

  private final KafkaProducer<?, byte[]> impl;
  private final MessageSerializer serializer = new MessageSerializer();
  private final String topic;

  public Producer(String topic, Properties additionalProperties) {
    this.topic = topic;
    this.impl = getKafkaProducer(additionalProperties);
  }

  public Producer(String topic, String brokerList) {
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    this.topic = topic;
    this.impl = getKafkaProducer(prop);
  }

  public void emit(Message msg) {
    emit(msg, System.currentTimeMillis() / SECONDS_TO_MILLS);
  }

  /**
   * Emit a message with specific timestamp.
   * @param msg The prototube message.
   * @param time seconds since 1970-01-01 00:00:00
   */
  public void emit(Message msg, long time) {
    byte[] payload = serializer.serialize(msg, time);
    impl.send(new ProducerRecord<>(topic, payload));
  }

  public void flush() {
    impl.flush();
  }

  @Override
  public void close() {
    impl.close();
  }

  private static KafkaProducer<?, byte[]> getKafkaProducer(Properties additionalProperties) {
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DEFAULT_BATCH_SIZE_CONFIG));
    prop.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(DEFAULT_RETRY_BACKOFF_MS));
    prop.putAll(additionalProperties);
    return new KafkaProducer<>(prop);
  }
}
