package cl.duoc.digitalfix.kafka.admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CrearTopicoRequest(
        @NotBlank @Pattern(regexp = "[a-zA-Z0-9._-]{3,120}") String nombre,
        @Min(1) @Max(48) Integer particiones,
        @Min(1) @Max(3) Integer replicas,
        @Min(60000) Long retencionMs,
        @Pattern(regexp = "delete|compact") String limpieza
) {}