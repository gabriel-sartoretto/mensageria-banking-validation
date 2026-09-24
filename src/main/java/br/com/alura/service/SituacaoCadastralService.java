package br.com.alura.service;

import br.com.alura.domain.Agencia;
import br.com.alura.domain.audit.Audit;
import br.com.alura.repository.SituacaoCadastralRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class SituacaoCadastralService {

    private final SituacaoCadastralRepository situacaoCadastralRepository;

    // Envio de mensagem pelo RabbitMQ
    private final Emitter<Audit> emitter;

    // Envio de mensagem pelo Kafka
    private final MutinyEmitter<br.com.alura.Agencia> mutinyEmitter;
    //private final ObjectMapper objectMapper;


    public SituacaoCadastralService(
            SituacaoCadastralRepository situacaoCadastralRepository,
            @Channel("notificacoes") Emitter<Audit> emitter,
            @Channel("remover-agencia-channel") MutinyEmitter<br.com.alura.Agencia> mutinyEmitter
            //ObjectMapper objectMapper
    ) {
        this.situacaoCadastralRepository = situacaoCadastralRepository;
        this.emitter = emitter;
        this.mutinyEmitter = mutinyEmitter;
        //this.objectMapper = objectMapper;
    }

    @WithTransaction
    public Uni<Void> alterar(Agencia agencia) {
        return situacaoCadastralRepository
                .update("situacaoCadastral = ?1 where cnpj = ?2",
                        agencia.getSituacaoCadastral(), agencia.getCnpj())
                .onItem().invoke(() -> {
                    //RabbitMQ
                    emitter.send(new Audit(agencia.getId(), agencia.getCnpj(), agencia.getSituacaoCadastral()));
                })
                // Kafka
                .call(() -> {
                    try {
                        if (agencia.getSituacaoCadastral().equals("INATIVO")) {
                            //Usado o objectMapper quando é enviado no formato Json
                            //String agenciaJson = objectMapper.writeValueAsString(agencia);
                            br.com.alura.Agencia agenciaConvertida = new br.com.alura.Agencia(
                                    agencia.getNome(),
                                    agencia.getRazaoSocial(),
                                    agencia.getCnpj(),
                                    agencia.getSituacaoCadastral());
                            return mutinyEmitter.send(agenciaConvertida);
                        }
                        return Uni.createFrom().voidItem();
                    } catch (Exception e) {
                        return Uni.createFrom().failure(e);
                    }
                })
                .replaceWithVoid();
    }
}
