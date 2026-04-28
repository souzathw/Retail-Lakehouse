# Raw Validation Notes

## Objetivo

Registrar as validações iniciais aplicadas sobre a camada raw do dataset Instacart.

## Validações implementadas na primeira versão

- existência das entidades esperadas
- existência de partições `ingestion_date=...`
- uso da partição mais recente
- existência do CSV esperado em cada entidade
- contagem de linhas de dados
- leitura do header
- comparação entre colunas encontradas e colunas esperadas
- detecção de arquivo vazio

## Entidades validadas

- orders
- products
- departments
- aisles
- order_products_prior
- order_products_train

## Próximos passos de qualidade

- persistir resumo da validação em arquivo
- validar duplicidade em chaves importantes
- validar tipos esperados
- comparar contagem entre source e raw
- criar checks preparatórios para Bronze