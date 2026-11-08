package cl.duoc.digitalfix.kafka.admin.controller;

import cl.duoc.digitalfix.kafka.admin.dto.OrdenEventoRequest;
import cl.duoc.digitalfix.kafka.admin.service.PublicadorDemo;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/demo")
public class DemoController {
    private final PublicadorDemo publisher;
    public DemoController(PublicadorDemo publisher) { this.publisher = publisher; }
    @PostMapping("/workorders")
    public Map<String,Object> publicar(@Valid @RequestBody OrdenEventoRequest evento) throws Exception {
        return publisher.enviar(evento);
    }
    // Solo para demostrar validación, reintentos y DLT durante la evaluación local.
    @PostMapping("/malformed-event")
    public Map<String,Object> malformado() throws Exception { return publisher.enviarMalformado(); }
}