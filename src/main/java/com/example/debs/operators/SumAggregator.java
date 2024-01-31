package com.example.debs.operators;

import com.example.debs.model.InputMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumAggregator implements AggregateFunction<InputMessage, Tuple2<Long, Long>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(InputMessage inputMessage, Tuple2<Long, Long> accumulator) {
        return new Tuple2<>(inputMessage.getVaultId(), accumulator.f1 + inputMessage.getFailure());
    }

    @Override
    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> accumulator1, Tuple2<Long, Long> accumulator2) {
        return new Tuple2<>(accumulator1.f0, accumulator1.f1 + accumulator2.f1);
    }
}
