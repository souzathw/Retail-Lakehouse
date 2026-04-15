# Instacart Data Dictionary — Initial Version

Este documento registra a visão inicial das entidades principais do dataset Instacart utilizadas no projeto.

## 1. orders.csv

Tabela principal de pedidos.

Colunas esperadas:
- `order_id`: identificador do pedido
- `user_id`: identificador do cliente
- `eval_set`: conjunto de avaliação (`prior`, `train`, `test`)
- `order_number`: número sequencial do pedido para o cliente
- `order_dow`: dia da semana do pedido
- `order_hour_of_day`: hora do pedido
- `days_since_prior_order`: dias desde o pedido anterior

## 2. products.csv

Tabela de produtos.

Colunas esperadas:
- `product_id`: identificador do produto
- `product_name`: nome do produto
- `aisle_id`: identificador do corredor
- `department_id`: identificador do departamento

## 3. departments.csv

Tabela de departamentos.

Colunas esperadas:
- `department_id`: identificador do departamento
- `department`: nome do departamento

## 4. aisles.csv

Tabela de corredores.

Colunas esperadas:
- `aisle_id`: identificador do corredor
- `aisle`: nome do corredor

## 5. order_products__prior.csv

Itens de pedidos anteriores.

Colunas esperadas:
- `order_id`: identificador do pedido
- `product_id`: identificador do produto
- `add_to_cart_order`: posição de inclusão no carrinho
- `reordered`: indicador de recompra

## 6. order_products__train.csv

Itens de pedidos do conjunto de treino.

Colunas esperadas:
- `order_id`: identificador do pedido
- `product_id`: identificador do produto
- `add_to_cart_order`: posição de inclusão no carrinho
- `reordered`: indicador de recompra

## Relações principais

- `orders.order_id` relaciona com `order_products__prior.order_id`
- `orders.order_id` relaciona com `order_products__train.order_id`
- `products.product_id` relaciona com tabelas de itens de pedido
- `products.aisle_id` relaciona com `aisles.aisle_id`
- `products.department_id` relaciona com `departments.department_id`

## Observações de modelagem

O dataset permite construir:
- fatos de pedidos
- fatos de itens por pedido
- dimensões de produto
- dimensões de corredor
- dimensões de departamento
- análises de recorrência e frequência de compra