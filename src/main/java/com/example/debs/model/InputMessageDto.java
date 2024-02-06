package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nullable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessageDto {
    private Long date;
    private String serial_number;
    private String model;
    private Long failure;
    private Long vault_id;
    private Smart smart;
}
