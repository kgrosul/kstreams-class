package ru.curs.homework.transformer;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.homework.util.StatefulTransformer;

import java.util.Optional;


@AllArgsConstructor
public class TotallingTransformer implements
        StatefulTransformer<String, Long,
                String, Long,
                String, Long> {

    private String store_name;

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public String storeName() {
        return store_name;
    }

    @Override
    public Serde<String> storeKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Long> storeValueSerde() {
        return new JsonSerde<>(Long.class);//Serdes.Long();
    }

    @Override
    public KeyValue<String, Long> transform(String key, Long value,
                                            KeyValueStore<String, Long> stateStore) {
        long current = Optional.ofNullable(stateStore.get(key)).orElse(0L);
        current += value;
        stateStore.put(key, current);
        return KeyValue.pair(key, current);
    }
}
