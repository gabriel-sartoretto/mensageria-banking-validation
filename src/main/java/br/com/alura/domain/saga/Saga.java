package br.com.alura.domain.saga;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
public class Saga {

    @Id
    private String id;

    //Para reporcessar se necessário
    private String entidade;

    @Enumerated(EnumType.STRING)
    private SagaStatus status;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    public Saga() {
    }

    public Saga(String id, String entidade, SagaStatus status, LocalDateTime createdAt) {
        this.id = id;
        this.entidade = entidade;
        this.status = status;
        this.createdAt = createdAt;
    }

    public String getId() {
        return id;
    }

    public String getEntidade() {
        return entidade;
    }

    public SagaStatus getStatus() {
        return status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
}
