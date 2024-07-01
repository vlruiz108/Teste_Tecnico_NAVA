# Teste Técnico NAVA

Este projeto implementa uma solução para calcular o saldo atualizado de contas correntes de clientes usando PySpark. O objetivo é apresentar o saldo atualizado de cada cliente por data, permitindo reprocessar e identificar alterações nos saldos entre os dias.

## Estrutura do Projeto


## Requisitos

- Python 3.7 ou superior
- PySpark

## Configuração do Ambiente

### 1. Criar e Ativar um Ambiente Virtual

Para criar um ambiente virtual e instalar as dependências:

```sh
python -m venv venv
.\venv\Scripts\activate
```
### 2. Instalar as Dependências
```sh
pip install -r requirements.txt
```

## Execução do Projeto
```sh
python main.py
```

## Detalhes da Implementação

O script main.py realiza as seguintes etapas:
1. Importa as bibliotecas necessárias, incluindo PySpark.
2. Criação da Sessão do Spark
3. Carrega os dados de saldo inicial e movimentações por data.
4. Cálcula os saldos:
    - Ordena por data e cliente
    - Cálculo do saldo inicial
    - Acumula as movimentações diárias para calcular o saldo final.
    - Reprocessa os saldos para considerar estornos.
    - Junta os saldos diários com as movimentações para obter o saldo final atualizado.
5. Exibe o resultado final com os saldos atualizados por cliente e data.
6. Encerra a sessão do Spark após o processamento.

## Resultados

Aqui estão os saldos atualizados de contas correntes de clientes por data:

| Data       | Cliente | Saldo Final |
|------------|---------|-------------|
| 02/04/2022 | Cliente 1 | 50,00 |
| 02/04/2022 | Cliente 2 | 238,95 |
| 02/04/2022 | Cliente 3 | 395,06 |
| ...        | ...     | ...         |
| 03/04/2022 | Cliente 1 | 150,00 |
| 03/04/2022 | Cliente 2 | 974,58 |
| 03/04/2022 | Cliente 3 | 1409,91 |
| ...        | ...     | ...         |

Os resultados mostram o saldo final de cada cliente para cada data processada, permitindo visualizar a evolução dos saldos ao longo do tempo e identificar movimentações e estornos conforme especificado no desafio.
