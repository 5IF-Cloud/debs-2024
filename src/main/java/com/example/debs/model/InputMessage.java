package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessage implements Serializable {
    private LocalDateTime date;
    private String serial_number;
    private String model;
    private Long failure;
    private Long vaultId;
    @Nullable
    private Smart smart;

    @Override
    public String toString() {
        return "InputMessage{" +
                "date=" + date +
                ", serial_number='" + serial_number + '\'' +
                ", model='" + model + '\'' +
                ", failure=" + failure +
                ", vaultId=" + vaultId +
                ", smart=" + smart +
                '}';
    }

    public long getEventTimeEpochMilli() {
        ZoneId zoneId = ZoneId.of("UTC");
        return date.atZone(zoneId).toEpochSecond() * 1000;
    }
}
