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
public class NFByVault {
    private LocalDateTime startTimeWindow;
    private Long vaultId;
    private Long numberOfFailures;

    @Override
    public String toString() {
        return "NFByVault{" +
                "startTimeWindow=" + startTimeWindow +
                ", vaultId=" + vaultId +
                ", numberOfFailures=" + numberOfFailures +
                '}';
    }
}
