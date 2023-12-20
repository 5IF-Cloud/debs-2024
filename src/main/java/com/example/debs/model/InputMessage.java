package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.time.ZoneId;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessage {
    private LocalDateTime date;
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
        ZoneId zoneId = ZoneId.of("UTC");
        return date.atZone(zoneId).toEpochSecond() * 1000;
    }
}
