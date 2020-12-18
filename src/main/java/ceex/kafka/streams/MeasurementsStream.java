package ceex.kafka.streams;

// The basic structure of this code is adapted from the examples in Stephane Maarek's Udemy course:
// https://www.udemy.com/course/kafka-streams/

// The handling of the state store is based on the following post on stackoverflow:
// https://stackoverflow.com/questions/58745670/kafka-compare-consecutive-values-for-a-key/58747294#58747294

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Properties;


public class MeasurementsStream {

    public static void main(String[] args) {

        // define key and value serializer and deserializer (serde)
        // key = String
        // value = Avro (GenericRecord)
        Serde<GenericRecord> value_serde = new GenericAvroSerde();
        value_serde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "http://localhost:8081/"),
                false);

        Serde<String> key_serde = Serdes.String();

        // set stream configurations
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "measurements-stream");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // AUTO_OFFSET_RESET_CONFIG = earliest --> used during development, change this according to your needs

        // add interceptors to properties for performance monitoring
        properties.put(
                StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        properties.put(
                StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        // create stream builder and add state store (key value store) to it (with above defined key value serde)
        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, GenericRecord>> storeBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("lastMeasurements"),
                        key_serde,
                        value_serde);

        builder.addStateStore(storeBuilder);

        // define logic of stream
        // K_STREAMS_MEASUREMENTS = source topic
        KStream<String, GenericRecord> stream = builder.stream("K_STREAMS_MEASUREMENTS", Consumed.with(key_serde, value_serde))
                .transformValues(() -> new ValueTransformerWithKey<String, GenericRecord, GenericRecord>() {
                    private KeyValueStore<String, GenericRecord> state;

                    // initiate state store
                    @Override
                    public void init(final ProcessorContext context) {
                        state = (KeyValueStore<String, GenericRecord>) context.getStateStore("lastMeasurements");
                    }

                    // reading message
                    @Override
                    public GenericRecord transform(final String key, final GenericRecord value) {
                        // get last message of the same key
                        GenericRecord prevValue = state.get(key);

                        GenericRecord returnvalue = null;

                        if (prevValue != null) {
                            // there might be a previous message, but it can have a larger timestamp than the current message
                            // this can happen when reprocessing messages (see offset_reset = earliest)
                            // in this case, update the state store with the current message and go to next message
                            if ((long) prevValue.get("TIME") > (long) value.get("TIME")) {
                                state.put(key, value);
                            }

                            else {
                                // if there is a previous message with the same key (and a smaller timestamp)...
                                // and the value for power is different, update end_time of the previous message with...
                                // the timestamp of the current message and update state store afterwards with value from current message
                                if (!prevValue.get("POWER").equals(value.get("POWER"))) {
                                    prevValue.put("END_TIME", (long) value.get("TIME"));

                                    state.put(key, value);

                                    // update returnvalue with value of previous message (with updated end_time)
                                    returnvalue = prevValue;
                                }

                                // if there is a previous message with the same key, but the value for power is the same...
                                // just return current message
                                else {
                                    returnvalue = value;
                                }
                            }
                        }
                        else {
                            // if there was no previous message with the same key, update state store with current message
                            state.put(key, value);
                        }
                        return returnvalue;
                    }

                    @Override
                    public void close() {
                    }
                }, "lastMeasurements");


        // MEASUREMENTS_KSTREAMS_OUTPUT = target topic
        stream.to("MEASUREMENTS_KSTREAMS_OUTPUT", Produced.with(key_serde, value_serde));

        // build and start stream
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        System.out.println(streams.toString());

        // safely close stream when shutdown command is issued
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
