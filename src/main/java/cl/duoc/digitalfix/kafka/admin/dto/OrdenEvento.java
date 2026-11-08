package cl.duoc.digitalfix.kafka.admin.dto;

public record OrdenEvento(
        String eventId,
        Long workOrderId,
        Long clienteId,
        String estado,
        Long tecnicoId,
        String timestamp
) {}