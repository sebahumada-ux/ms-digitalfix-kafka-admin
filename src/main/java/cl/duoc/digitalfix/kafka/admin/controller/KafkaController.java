package cl.duoc.digitalfix.kafka.admin.controller;

import cl.duoc.digitalfix.kafka.admin.dto.ConfigTopicoRequest;
import cl.duoc.digitalfix.kafka.admin.dto.CrearTopicoRequest;
import cl.duoc.digitalfix.kafka.admin.service.GruposService;
import cl.duoc.digitalfix.kafka.admin.service.TopicoService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
    private final TopicoService topicos;
    private final GruposService grupos;
    public KafkaController(TopicoService topicos, GruposService grupos) {
        this.topicos = topicos; this.grupos = grupos;
    }
    @GetMapping("/topics")
    public Set<String> listarTopicos() throws Exception { return topicos.listar(); }
    @GetMapping("/topics/{nombre:.+}")
    public Map<String,Object> detalle(@PathVariable String nombre) throws Exception { return topicos.detalle(nombre); }
    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("/topics")
    public Map<String,Object> crear(@Valid @RequestBody CrearTopicoRequest request) throws Exception {
        return topicos.crear(request);
    }
    @PatchMapping("/topics/{nombre:.+}/config")
    public Map<String,Object> cambiarConfig(@PathVariable String nombre,
                                            @Valid @RequestBody ConfigTopicoRequest request) throws Exception {
        return topicos.modificarConfig(nombre,request);
    }
    @PostMapping("/topics/{nombre:.+}/partitions")
    public Map<String,Object> aumentar(@PathVariable String nombre,
                                       @RequestParam @Min(1) int cantidad) throws Exception {
        return topicos.aumentarParticiones(nombre,cantidad);
    }
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping("/topics/{nombre:.+}")
    public void eliminar(@PathVariable String nombre,@RequestParam(defaultValue="false") boolean confirmar)
            throws Exception { topicos.eliminar(nombre,confirmar); }
    @GetMapping("/groups")
    public java.util.List<String> grupos() throws Exception { return grupos.listar(); }
    @GetMapping("/groups/{grupo:.+}/lag")
    public Map<String,Object> lag(@PathVariable String grupo) throws Exception { return grupos.lag(grupo); }
}