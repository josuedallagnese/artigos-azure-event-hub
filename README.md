#### Artigo publicado em https://josuedallagnese.medium.com/data-ingestion-com-azure-event-hubs-a5645843f7a3

# Exemplo sobre Azure Event Hubs

## Pré-requisitos
Assinatura no portal Azure para criação de um Event Hubs Namespace e uma Storage Account:

1) Crie uma **Storage Account** com o nome e o local que preferir, com dois containers privado chamado "**case2**" e "**case3**".
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
                "BlobContainerName": "case2",
                "BlobStorageConnectionString": "<Conexão Storage Account>"
            },
            {
                "HubName": "case3",
                "ConsumerGroup": "$Default",
                "BlobContainerName": "case3",
                "NumberOfMessages": 50000,
                "Chunks": 2500,
                "CheckpointAt": 25,
                "BlobStorageConnectionString": "<Conexão Storage Account>"
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
[Option 1] - Case 1: consuming one partition ('0') with PartitionReceiver in batch size of maximum 10 itens
[Option 2] - Case 2: consuming each partition ('0','1','2') in sequence with EventHubConsumerClient
[Option 3] - Case 2: consuming a single partition ('0','1','2') EventHubConsumerClient
[Option 4] - Case 2: all partitions with EventProcessorClient
[Option 5] - Case 2: Read partition ('0','1','2') in bacth size using PartitionReceiver ...
[Option 6] - Case 3: Read all partition with Event Processor Client and save in local database...

[0] - Exit
```

Cada caso explora um hub com uma estrutura diferente, onde mensagens são produzidas pelo projeto de **Producer** e
consumidas de diferentes formas pelo projeto de **Consumer**. 

Sempre execute o projeto de Producer e o projeto de Consumer com a opção do caso desejado: 1, 2 ou 3.

Ao escolher o "caso3" de **Consumer**, opção 6 do console, suba quantas instâncias quiser do **Consumer** usando a opção Control + F5 pelo visual studio.

Este exemplo irá criar uma base de dados chamada "EventHub" com uma tabela "Case3" sobre a instância PostgreSQL informada contendo as mensagens recebidas de cada partição.

O parâmetro no arquivo de configuração "**CheckpointAt**" está em 25 messagens para que a visualização do processamento possa ser feita.
