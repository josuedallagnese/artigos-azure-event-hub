#### Artigo publicado em https://josuedallagnese.medium.com/data-ingestion-com-azure-event-hubs-a5645843f7a3

# Exemplo sobre Azure Event Hubs

## Pré-requisitos
Assinatura no portal Azure para criação de um Event Hubs Namespace e uma Storage Account:

1) Crie uma **Storage Account** com o nome e o local que preferir, com um container privado chamado "**event-hub**".
2) Crie um **Event Hubs Namespace** com o nome e o local que preferir, definindo o Pricing tier para **Standard** e o **Throughput Units** para 2.
3) Dentro do namespace criado, adicione 3 hubs chamados respectivamente de "**case1**" com 1 partição, "**case2**" com 3 partições e "**case3**" com 4 partições.
4) Crie um **Azure Database for PostgreSQL Server** na sua conta Azure ou então suba na sua máquina local uma instância como preferir (docker é uma boa opção).

## Configuração

Existem dois projetos nesta solução chamados **EventHub.Consumer** e **EventHub.Producer**.

Dentro de cada projeto configure o arquivo **appsettings.Development.json** conforme a estrutura a seguir:
```json
{
    "NpgsqlConnectionString": "<Conexão PostgreSQL>",
    "EventHub": {
        "ConnectionString": "<Conexão Event Hub Namespace - encontre na sessão Shared access policies dentro do Namespace do Event Hub criado>",
        "Hubs": [
            {
                "HubName": "case1",
                "ConsumerGroup": "$Default",
                "NumberOfMessages": 150
            },
            {
                "HubName": "case2",
                "ConsumerGroup": "$Default",
                "NumberOfMessages": 150,
                "BlobContainerName": "event-hub",
                "BlobStorageConnectionString": "<Conexão Storage Account>"
            },
            {
                "HubName": "case3",
                "ConsumerGroup": "$Default",
                "NumberOfMessages": 50000,
                "NumberOfMessagesByChunk": 2500
            }
        ]
    }
}
```

## Execução
Ao rodar o projeto **EventHub.Producer** você verá 3 opções como estas:
```
What job do you want?
[1] - Case 1: producing messages to hub with a single partition
[2] - Case 2: producing messages to hub with three partitions
[3] - Case 3: producing messages to hub with four partitions

[0] - Exit
```

Ao rodar o projeto **EventHub.Consumer** você verá 5 opções como estas:
```
What job do you want?
[1] - Case 1: consuming one partition ('0') with PartitionReceiver in batch size of maximum 10 itens
[2] - Case 2: consuming each partition ('0','1','2') in sequence with EventHubConsumerClient
[3] - Case 2: consuming a single partition EventHubConsumerClient
[4] - Case 2: all partitions with EventProcessorClient
[5] - Case 3: Read for partition in bacth size using PartitionReceiver and save in local database...

[0] - Exit
```

Cada caso explora um hub com uma estrutura diferente, onde mensagens são produzidas pelo projeto de **Producer** e
consumidas de diferentes formas pelo projeto de **Consumer**. 

Sempre execute o projeto de Producer e o projeto de Consumer com a opção do caso desejado: 1, 2 ou 3.

Ao executar o "caso3" de **Consumer**, opção 5 do console, você poderá escolher qual partição você irá querer consumir.

Sugiro subir 4 instâncias do **Consumer** (Control + F5 pelo visual studio) e opção 5 do console informando respectivamente as partições 0, 1, 2 e 3 como você criou anteriormente em sua conta Azure.

Este exemplo irá criar uma base de dados chamada "EventHub" com uma tabela "Case3" na instância PostgreSQL informada contendo as mensagens recebidas de cada partição.
