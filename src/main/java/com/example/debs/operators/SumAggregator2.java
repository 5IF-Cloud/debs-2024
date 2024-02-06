package com.example.debs.operators;

import com.example.debs.model.Centroid;
import com.example.debs.model.ClusteredHardware;
import com.example.debs.model.Smart;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumAggregator2 implements AggregateFunction<ClusteredHardware, Centroid, Centroid> {
    @Override
    public Centroid createAccumulator() {
        Centroid centroid = new Centroid();
        centroid.setClusterId(-1L);
        Smart smart = new Smart();
        smart.initSmart();
        centroid.setSmart(smart);
        return centroid;
    }

    @Override
    public Centroid add(ClusteredHardware clusteredHardware, Centroid accumulator) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(clusteredHardware.getClusterId());
        Smart smartHardware = clusteredHardware.getSmart();
        Smart smartAccumulator = accumulator.getSmart();
        Smart smart = smartHardware.plus(smartAccumulator);
        centroid.setSmart(smart);
        return centroid;
    }

    @Override
    public Centroid getResult(Centroid accumulator) {
        return accumulator;
    }

    @Override
    public Centroid merge(Centroid accumulator1, Centroid accumulator2) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(accumulator1.getClusterId());
        Smart smart1 = accumulator1.getSmart();
        Smart smart2 = accumulator2.getSmart();
        Smart smart = smart1.plus(smart2);
        centroid.setSmart(smart);
        return centroid;
    }
}
