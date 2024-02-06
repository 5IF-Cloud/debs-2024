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
public class ClusteredHardware {
    private LocalDateTime date;
    private String serial_number;
    private String model;
    private Long failure;
    private Long vaultId;
    private Smart smart;
    private Long clusterId;

    @Override
    public String toString() {
        return "ClusteredHardware{" +
                "date=" + date +
                ", serial_number='" + serial_number +
                ", model='" + model +
                ", failure=" + failure +
                ", vaultId=" + vaultId +
                ", clusterId=" + clusterId +
                '}';
    }
}
