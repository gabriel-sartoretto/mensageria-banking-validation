package br.com.alura.domain.saga;

public enum SagaStatus {
    OPEN,
    // O consumidor encontrou a agência e a removeu
    COMPLETED,
    // A mensagem chegou, mas a agência já não existia (ex.: reenvio do resync): fechada sem ação
    IGNORED,
    ERROR,
}
