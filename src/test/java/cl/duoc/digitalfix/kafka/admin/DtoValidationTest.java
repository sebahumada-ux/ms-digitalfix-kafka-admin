package cl.duoc.digitalfix.kafka.admin;

import cl.duoc.digitalfix.kafka.admin.dto.CrearTopicoRequest;
import cl.duoc.digitalfix.kafka.admin.dto.OrdenEventoRequest;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DtoValidationTest {

    private final Validator validator =
            Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void rechazaOrdenSinId() {
        assertFalse(validator.validate(
                new OrdenEventoRequest(null, 5L, "CERRADA", 8L)
        ).isEmpty());
    }

    @Test
    void aceptaTopicoConTresReplicas() {
        assertTrue(validator.validate(
                new CrearTopicoRequest("demo.prueba", 3, 3, 86400000L, "delete")
        ).isEmpty());
    }

    @Test
    void rechazaReplicaMayorTres() {
        assertFalse(validator.validate(
                new CrearTopicoRequest("demo.prueba", 3, 4, 86400000L, "delete")
        ).isEmpty());
    }
}