package br.com.alura.service;

import br.com.alura.domain.Agencia;
import br.com.alura.domain.audit.Audit;
import br.com.alura.domain.saga.Saga;
import br.com.alura.domain.saga.SagaStatus;
import br.com.alura.repository.SituacaoCadastralRepository;
import br.com.alura.repository.saga.SagaRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.time.LocalDateTime;
import java.util.UUID;

@ApplicationScoped
public class SituacaoCadastralService {

    private final SituacaoCadastralRepository situacaoCadastralRepository;

    // Envio de mensagem pelo RabbitMQ
    private final Emitter<Audit> emitter;

    // Envio de mensagem pelo Kafka
    private final MutinyEmitter<br.com.alura.Agencia> mutinyEmitter;

    private final ObjectMapper objectMapper;
    private final SagaRepository sagaRepository;

    public SituacaoCadastralService(
            SituacaoCadastralRepository situacaoCadastralRepository,
            @Channel("notificacoes") Emitter<Audit> emitter,
            @Channel("remover-agencia-channel") MutinyEmitter<br.com.alura.Agencia> mutinyEmitter, SagaRepository sagaRepository,
            ObjectMapper objectMapper
    ) {
        this.situacaoCadastralRepository = situacaoCadastralRepository;
        this.emitter = emitter;
        this.mutinyEmitter = mutinyEmitter;
        this.objectMapper = new ObjectMapper();
        this.sagaRepository = sagaRepository;
    }

    @WithTransaction
    public Uni<Void> alterar(Agencia agencia) {
        // O "situacaoCadastral <> ?1" faz o update só alterar a linha se a situação realmente mudar.
        // Como a condição é avaliada no próprio UPDATE, cliques repetidos (mesmo simultâneos)
        // resultam em uma única alteração, e só ela gera auditoria e saga
        return situacaoCadastralRepository
                .update("situacaoCadastral = ?1 where cnpj = ?2 and situacaoCadastral <> ?1",
                        agencia.getSituacaoCadastral(), agencia.getCnpj())
                .chain(linhasAlteradas -> {
                    if (linhasAlteradas == 0) {
                        // Agência já estava nessa situação (ou CNPJ inexistente): nada a auditar nem a sincronizar
                        return Uni.createFrom().voidItem();
                    }
                    //RabbitMQ
                    emitter.send(new Audit(agencia.getId(), agencia.getCnpj(), agencia.getSituacaoCadastral()));
                    // Kafka
                    try {
                        if (agencia.getSituacaoCadastral().equals("INATIVO")) {
                            //Usado o objectMapper quando é enviado no formato Json
                            //String agenciaJson = objectMapper.writeValueAsString(agencia);
                            // Um id por operação: cada inativação gera uma saga nova e o histórico fica preservado
                            String sagaId = UUID.randomUUID().toString();
                            return sagaRepository.persist(new Saga(
                                    sagaId,
                                    objectMapper.writeValueAsString(agencia),
                                    SagaStatus.OPEN,
                                    LocalDateTime.now()
                            )).call(() -> {
                                // O sagaId vai na mensagem para o consumidor fechar a saga certa
                                br.com.alura.Agencia agenciaConvertida = new br.com.alura.Agencia(
                                        agencia.getNome(),
                                        agencia.getRazaoSocial(),
                                        agencia.getCnpj(),
                                        agencia.getSituacaoCadastral(),
                                        sagaId);
                                return mutinyEmitter.send(agenciaConvertida);
                            });
                        }
                        return Uni.createFrom().voidItem();
                    } catch (Exception e) {
                        return Uni.createFrom().failure(e);
                    }
                })
                .replaceWithVoid();
    }
}
