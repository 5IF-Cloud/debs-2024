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
}
