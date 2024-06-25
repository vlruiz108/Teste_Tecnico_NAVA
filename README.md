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

## Estrutura do Script

O script `main.py` segue os seguintes passos:
1. Importação das Bibliotecas Necessárias
2. Criação da Sessão do Spark
3. Definição do Esquema dos Dados e Simulação das Movimentações Diárias
4. Cálcula os saldos:
    - Ordenação por data e cliente
    - Cálculo do saldo inicial
    - Acumulação das movimentações para calcular o saldo final
    - Reprocessamento de saldos para considerar estornos
    - Juntada dos saldos diários com a movimentação para o saldo final atualizado
5. Exibição do Saldo Final
6. Encerramento da Sessão do Spark

Este projeto foi desenvolvido como parte de um teste técnico para demonstrar habilidades em processamento distribuído com PySpark e manipulação de dados complexos para cálculo de saldos em contas correntes.
