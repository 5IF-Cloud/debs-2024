package com.example.debs.operators;

import com.example.debs.model.InputMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CountAggregator implements AggregateFunction<InputMessage, Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(InputMessage inputMessage, Tuple2<Long, Long> accumulator) {
        return Tuple2.of(inputMessage.getVaultId(), accumulator.f1 + 1L);
    }

    @Override
    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return Tuple2.of(a.f0, a.f1 + b.f1);
    }
}
