package cl.duoc.digitalfix.kafka.admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

public record ConfigTopicoRequest(
        @Min(60000) Long retencionMs,
        @Pattern(regexp = "delete|compact") String limpieza
) {}