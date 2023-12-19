package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessage {
    private Instant date;
    private Boolean isFailure;
    private Long vaultId;

    @Override
    public String toString() {
        return "InputMessage{" +
                "date=" + date +
                ", isFailure=" + isFailure +
                ", vaultId=" + vaultId +
                '}';
    }

    public long getEventTimeEpochMilli() {
        return date.toEpochMilli();
    }
}
