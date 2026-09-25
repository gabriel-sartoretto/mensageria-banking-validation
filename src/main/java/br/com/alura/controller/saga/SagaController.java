package br.com.alura.controller.saga;

import br.com.alura.domain.saga.SagaStatus;
import br.com.alura.repository.saga.SagaRepository;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

@Path("/saga")
public class SagaController {

    private final SagaRepository sagaRepository;

    public SagaController(SagaRepository sagaRepository) {
        this.sagaRepository = sagaRepository;
    }

    @PUT
    @Path("/sucesso")
    @WithTransaction
    public Uni<Void> fecharSagaSucesso(String id) {
        return sagaRepository.update("status = ?1 where id = ?2", SagaStatus.COMPLETED, id)
                .replaceWithVoid();
    }

    @PUT
    @Path("/ignorada")
    @WithTransaction
    public Uni<Void> fecharSagaIgnorada(String id) {
        return sagaRepository.update("status = ?1 where id = ?2", SagaStatus.IGNORED, id)
                .replaceWithVoid();
    }

    @PUT
    @Path("/erro")
    @WithTransaction
    public Uni<Void> fecharSagaErro(String id) {
        return sagaRepository.update("status = ?1 where id = ?2", SagaStatus.ERROR, id)
                .replaceWithVoid();
    }
}
