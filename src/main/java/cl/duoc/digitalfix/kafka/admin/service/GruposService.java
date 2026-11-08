package cl.duoc.digitalfix.kafka.admin.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class GruposService {
    private final AdminClient admin;
    public GruposService(AdminClient admin) { this.admin = admin; }

    public List<String> listar() throws Exception {
        return admin.listConsumerGroups().all().get(15, TimeUnit.SECONDS).stream()
                .map(ConsumerGroupListing::groupId).sorted().toList();
    }

    public Map<String, Object> lag(String grupo) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> consumidos = admin.listConsumerGroupOffsets(grupo)
                .partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetSpec> ultimos = new HashMap<>();
        consumidos.keySet().forEach(tp -> ultimos.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales = ultimos.isEmpty()
                ? Map.of() : admin.listOffsets(ultimos).all().get(15, TimeUnit.SECONDS);
        List<Map<String,Object>> particiones = new ArrayList<>();
        long total = 0;
        for (var entry : consumidos.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offsetConsumido = entry.getValue().offset();
            long ultimo = finales.get(tp).offset();
            long atraso = Math.max(0, ultimo - offsetConsumido);
            total += atraso;
            particiones.add(Map.of("topico", tp.topic(), "particion", tp.partition(),
                    "offsetConsumido", offsetConsumido, "offsetFinal", ultimo, "lag", atraso));
        }
        particiones.sort(Comparator.comparing(x -> x.get("topico").toString() + "-" + x.get("particion")));
        return Map.of("grupo", grupo, "lagTotal", total, "particiones", particiones);
    }
}