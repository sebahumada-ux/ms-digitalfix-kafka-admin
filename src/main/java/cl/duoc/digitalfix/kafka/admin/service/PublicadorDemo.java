package cl.duoc.digitalfix.kafka.admin.service;

import cl.duoc.digitalfix.kafka.admin.dto.OrdenEvento;
import cl.duoc.digitalfix.kafka.admin.dto.OrdenEventoRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class PublicadorDemo {
    private static final Set<String> ESTADOS = Set.of("CREADA", "ASIGNADA", "EN_DESPLAZAMIENTO",
            "EN_EJECUCION", "CERRADA", "CANCELADA");
    private final KafkaTemplate<String,String> kafka;
    private final ObjectMapper mapper;
    private final String topico;

    public PublicadorDemo(KafkaTemplate<String,String> kafka, ObjectMapper mapper,
                          @Value("${app.kafka.topics.workorders}") String topico) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.topico = topico;
    }

    public Map<String,Object> enviar(OrdenEventoRequest request) throws Exception {
        String estado = request.estado().trim().toUpperCase();
        if (!ESTADOS.contains(estado)) throw new IllegalArgumentException("Estado no vÃ¡lido: " + estado);
        if (request.tecnicoId() != null && request.tecnicoId() <= 0)
            throw new IllegalArgumentException("tecnicoId debe ser positivo");
        OrdenEvento evento = new OrdenEvento(UUID.randomUUID().toString(), request.workOrderId(),
                request.clienteId(), estado, request.tecnicoId(), Instant.now().toString());
        return publicar(String.valueOf(request.workOrderId()), mapper.writeValueAsString(evento));
    }

    public Map<String,Object> enviarMalformado() throws Exception {
        return publicar("demo-error", "{esto no es json}");
    }

    private Map<String,Object> publicar(String clave, String json) throws Exception {
        RecordMetadata meta = kafka.send(topico, clave, json).get(15, TimeUnit.SECONDS).getRecordMetadata();
        return Map.of("topico", meta.topic(), "particion", meta.partition(), "offset", meta.offset(),
                "clave", clave, "mensaje", "Evento aceptado por Kafka");
    }
}