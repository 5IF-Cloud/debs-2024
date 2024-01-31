package com.example.debs.operators;

import com.example.debs.model.InputMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CountAggregator implements AggregateFunction<InputMessage, Tuple2<String, Long>, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> createAccumulator() {
        return new Tuple2<>("", 0L);
    }

    @Override
    public Tuple2<String, Long> add(InputMessage inputMessage, Tuple2<String, Long> accumulator) {
        return new Tuple2<>(inputMessage.getModel(), accumulator.f1 + 1L);
    }

    @Override
    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
        return new Tuple2<>(a.f0, a.f1 + b.f1);
    }
}
