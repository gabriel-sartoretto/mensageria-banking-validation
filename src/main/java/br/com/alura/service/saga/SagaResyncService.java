package br.com.alura.service.saga;

import br.com.alura.domain.Agencia;
import br.com.alura.repository.saga.SagaRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;

import java.time.LocalDateTime;

@ApplicationScoped
public class SagaResyncService {

    private final MutinyEmitter<br.com.alura.Agencia> kafkaEmitter;

    private final SagaRepository sagaRepository;

    private final ObjectMapper objectMapper;

    private final Vertx vertx;

    public SagaResyncService(
            @Channel("remover-agencia-channel")
            MutinyEmitter<br.com.alura.Agencia> kafkaEmitter,
            SagaRepository sagaRepository,
            ObjectMapper objectMapper,
            Vertx vertx
    ) {
        this.kafkaEmitter = kafkaEmitter;
        this.sagaRepository = sagaRepository;
        this.objectMapper = new ObjectMapper();
        this.vertx = vertx;
    }

    // delayed: a primeira execução espera a aplicação subir; sem isso o resync pode rodar
    // antes do canal Kafka estar pronto e falhar com SRMSG00019
    @Scheduled(every = "10s", delayed = "10s")
    public void resync() {
        vertx.runOnContext(() -> {

            LocalDateTime limite = LocalDateTime.now().minusMinutes(2);

            sagaRepository.listByCreatedAtAndStatus(limite)
                    .subscribe().with(sagas -> {
                        sagas.forEach(saga -> {
                            try {
                                // O JSON salvo na saga vem da entidade do domínio (tem "id"), não da classe Avro
                                Agencia agenciaConvertida = objectMapper.readValue(saga.getEntidade(), Agencia.class);
                                kafkaEmitter.sendAndForget(new br.com.alura.Agencia(
                                        agenciaConvertida.getNome(),
                                        agenciaConvertida.getRazaoSocial(),
                                        agenciaConvertida.getCnpj(),
                                        agenciaConvertida.getSituacaoCadastral(),
                                        saga.getId()
                                ));
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        });
                    });
        });
    }
}
