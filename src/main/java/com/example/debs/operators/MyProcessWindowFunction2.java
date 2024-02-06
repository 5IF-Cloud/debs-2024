package com.example.debs.operators;

import com.example.debs.model.Centroid;
import com.example.debs.model.Smart;
import com.example.debs.model.TimeCentroid;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class MyProcessWindowFunction2 extends ProcessWindowFunction<Centroid, TimeCentroid, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Centroid> input, Collector<TimeCentroid> out) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(input.iterator().next().getClusterId());
        Smart smart = new Smart();
        smart.initSmart();
        for (Centroid in : input) {
            Smart inSmart = in.getSmart();
            Smart divideInSmart = inSmart.divide(input.spliterator().estimateSize());
            smart = smart.plus(divideInSmart);
        }
        TimeCentroid timeCentroid = new TimeCentroid();
        // Set dayEnd to startWindow + 1 day
        LocalDateTime dayEnd = LocalDateTime.ofEpochSecond(context.window().getStart() / 1000 + 86400, 0, ZoneOffset.UTC);
        timeCentroid.setDayEnd(dayEnd);
        timeCentroid.setClusterId(centroid.getClusterId());
        timeCentroid.setSmart(smart);
        out.collect(timeCentroid);
    }
}
