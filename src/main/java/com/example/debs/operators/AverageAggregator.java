package com.example.debs.operators;

import com.example.debs.model.Centroid;
import com.example.debs.model.ClusteredHardware;
import com.example.debs.model.Smart;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class AverageAggregator implements AggregateFunction<ClusteredHardware, Tuple2<Centroid, Long>, Centroid> {
    @Override
    public Tuple2<Centroid, Long> createAccumulator() {
        Centroid centroid = new Centroid();
        centroid.setClusterId(-1L);
        Smart smart = new Smart();
        smart.initSmart();
        centroid.setSmart(smart);
        return new Tuple2<>(centroid, 0L);
    }

    @Override
    public Tuple2<Centroid, Long> add(ClusteredHardware clusteredHardware, Tuple2<Centroid, Long> accumulator) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(clusteredHardware.getClusterId());
        Smart smartHardware = clusteredHardware.getSmart();
        Smart smartAccumulator = accumulator.f0.getSmart();
        Smart smart = smartHardware.plus(smartAccumulator);
        centroid.setSmart(smart);
        return new Tuple2<>(centroid, accumulator.f1 + 1);
    }

    @Override
    public Centroid getResult(Tuple2<Centroid, Long> accumulator) {
        if (accumulator.f1 == 0) {
            return accumulator.f0;
        }
        Centroid centroid = new Centroid();
        centroid.setClusterId(accumulator.f0.getClusterId());
        Smart smart = accumulator.f0.getSmart().divide(accumulator.f1);
        centroid.setSmart(smart);
        return centroid;
    }

    @Override
    public Tuple2<Centroid, Long> merge(Tuple2<Centroid, Long> a, Tuple2<Centroid, Long> b) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(a.f0.getClusterId());
        Smart smartA = a.f0.getSmart();
        Smart smartB = b.f0.getSmart();
        Smart smart = smartA.plus(smartB);
        centroid.setSmart(smart);
        return new Tuple2<>(centroid, a.f1 + b.f1);
    }
}
