package com.example.debs.operators;

import com.example.debs.model.NFByVault;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class NewProcessWindowFunction extends ProcessWindowFunction<Tuple2<Long, Long>, NFByVault, Long, TimeWindow> {

    @Override
    public void process(Long key, Context context, Iterable<Tuple2<Long, Long>> nbFailuresByVault, Collector<NFByVault> output) {
        for (Tuple2<Long, Long> nbFailures : nbFailuresByVault) {
            // Convert start time window to LocalDateTime
            long startTimeWindow = context.window().getStart();
            // Convert Long milliseconds to LocalDateTime
            LocalDateTime startTime = LocalDateTime.ofEpochSecond(startTimeWindow / 1000, 0, ZoneOffset.UTC);

            Long clusterId = -1L;
            NFByVault nfByVault = new NFByVault(startTime, key, nbFailures.f1, clusterId);
            output.collect(nfByVault);
        }
    }
}
