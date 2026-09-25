# banking-validation

Microsserviço que mantém a **situação cadastral das agências** (ATIVO / INATIVO) e publica as mudanças para os
demais serviços. É o producer de mensagens do sistema e o orquestrador da **saga** de remoção de agências.

Faz parte do projeto [kafka-rabbitmq-banking](https://github.com/gabriel-sartoretto/kafka-rabbitmq-banking), junto com
[banking-service](https://github.com/gabriel-sartoretto/mensageria-banking-service) e
[banking-audit](https://github.com/gabriel-sartoretto/mensageria-banking-audit).

**Stack:** Java 21 · Quarkus 3.29 · Hibernate Reactive Panache · PostgreSQL · RabbitMQ · Kafka + Avro/Schema Registry · Quarkus Scheduler

## O que ele faz

Quando a situação de uma agência muda (`PUT /situacao-cadastral`):

1. Atualiza o banco **somente se a situação realmente mudou** (o `UPDATE` tem `situacaoCadastral <> ?`), então
   requisições repetidas não geram efeitos duplicados.
2. Envia uma mensagem de auditoria pelo **RabbitMQ** (exchange `notificacoes`, routing key `agencia.change_status`),
   consumida pelo banking-audit.
3. Se a agência ficou **INATIVO**, abre uma saga (`status = OPEN`, id UUID) e publica a agência no **Kafka**
   (tópico `remover-agencia-avro`, formato Avro com o `sagaId`), consumida pelo banking-service, que remove a agência
   e depois chama `/saga/*` aqui para fechar a saga.
4. Um job (`SagaResyncService`, a cada 10s) reenvia ao Kafka as sagas que continuam `OPEN` há mais de 2 minutos.

Status possíveis da saga: `OPEN`, `COMPLETED` (removida), `IGNORED` (agência já não existia), `ERROR`.

## Endpoints (porta 8181)

| Método | Caminho | Descrição |
|---|---|---|
| `POST` | `/situacao-cadastral` | Cadastra uma agência |
| `GET` | `/situacao-cadastral` | Lista todas |
| `GET` | `/situacao-cadastral/{cnpj}` | Busca por CNPJ (204 se não existir). Usado pelo banking-service |
| `PUT` | `/situacao-cadastral` | Altera a situação (dispara RabbitMQ e, se INATIVO, Kafka + saga) |
| `PUT` | `/saga/sucesso` · `/saga/ignorada` · `/saga/erro` | Fecha a saga; o body é o id da saga |

Exemplo:

```bash
curl -X PUT localhost:8181/situacao-cadastral -H "Content-Type: application/json" \
  -d '{"id":1,"nome":"Agencia BSB","razaoSocial":"Asa Norte AGENCIA BSB","cnpj":"15130254000100","situacaoCadastral":"INATIVO"}'
```

## Rodando localmente

O `docker-compose.yml` deste repositório sobe **a infraestrutura de todo o projeto**:

| Serviço | Porta |
|---|---|
| PostgreSQL (banco `agencia`, com as tabelas `agencia` e `saga` do `init.sql`) | 5432 |
| RabbitMQ (management em http://localhost:15672, guest/guest) | 5672 / 15672 |
| Zookeeper | 2181 |
| Kafka | 9092 |
| Schema Registry | 8081 |

```bash
docker compose up -d
./mvnw quarkus:dev
```

> O container da própria API (`joao0212/banking-validation:v2`) está comentado no compose de propósito: é a imagem
> do curso, sem as alterações locais, e ocuparia a porta 8181. Rode a API pela IDE ou pelo `quarkus:dev`.

## Schema Avro

`src/main/avro/Agencia.avsc` gera a classe `br.com.alura.Agencia`. O mesmo arquivo existe no banking-service:
qualquer mudança precisa ser feita **nos dois** e manter compatibilidade (novos campos com `default`, como `sagaId`).

## Build

```bash
./mvnw package                                  # target/quarkus-app/quarkus-run.jar
./mvnw package -Dquarkus.package.jar.type=uber-jar
./mvnw package -Dnative                         # executável nativo (requer GraalVM)
```
