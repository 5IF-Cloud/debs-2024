package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TimeCentroid {
    private LocalDateTime dayEnd;
    private Long clusterId;
    private Smart smart;

    @Override
    public String toString() {
        return "TimeCentroid{" +
                "dayEnd=" + dayEnd +
                ", clusterId=" + clusterId +
                ", smart=" + smart +
                '}';
    }
}
