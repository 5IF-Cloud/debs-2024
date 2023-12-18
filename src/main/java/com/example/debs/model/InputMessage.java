package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessage {
    private Long date;
    private Long failure;
    private Long vault_id;

    @Override
    public String toString() {
        return "InputMessage{" +
                "date=" + date +
                ", failure=" + failure +
                ", vault_id=" + vault_id +
                '}';
    }
}
