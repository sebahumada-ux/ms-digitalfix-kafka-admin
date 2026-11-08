package cl.duoc.digitalfix.kafka.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record OrdenEventoRequest(
        @NotNull @Positive Long workOrderId,
        @NotNull @Positive Long clienteId,
        @NotBlank String estado,
        Long tecnicoId
) {}