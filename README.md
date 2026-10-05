# Ampere

**Importa o CSV de vendas, diz exatamente o que havia de errado nele e devolve os indicadores prontos.**

Toda empresa que vende alguma coisa tem uma planilha exportada do ERP com data em três
formatos diferentes, preço com vírgula, UF digitada errada e linha repetida. O caminho
normal é alguém limpar isso na mão todo mês. O Ampere faz a limpeza, rejeita só a linha
ruim — nunca o arquivo inteiro —, explica o motivo de cada rejeição e expõe faturamento,
ticket médio, ranking de produtos e evolução mensal por uma API REST, com painel próprio
na raiz.

O sistema inteiro cabe em um arquivo: `ampere.py`.

![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009485)
![Licença](https://img.shields.io/badge/licen%C3%A7a-MIT-black)

---

## Rodando

```bash
pip install -r requirements.txt
python ampere.py dados     # gera os CSVs de exemplo em data/
python ampere.py           # sobe em http://localhost:8000
```

Abra <http://localhost:8000>, escolha `data/vendas_exemplo.csv` e clique em **Importar CSV**.
Depois importe `data/vendas_com_erros.csv` para ver o relatório de rejeições em ação.
A documentação interativa fica em <http://localhost:8000/docs>.

---

## O que ele resolve

| Problema do arquivo real | O que o Ampere faz |
| --- | --- |
| `15/03/2026`, `2026-03-15`, `15-03-2026` no mesmo arquivo | Reconhece os quatro formatos mais comuns |
| `1.234,56` e `1234.56` misturados | Normaliza o padrão BR e o padrão US |
| Separador `;`, `,`, tab ou `\|` | Detecta sozinho |
| Arquivo em UTF-8 ou Latin-1 (Excel brasileiro) | Tenta os dois |
| Cabeçalho `Qtde`, `Valor Unitário`, `Razão Social` | Mapeia apelidos para o nome canônico |
| Uma linha corrompida no meio de 5.000 | Rejeita só ela e devolve número da linha e motivo |
| Mesmo relatório importado duas vezes | Impressão digital SHA-256 por venda: nada duplica |
| Quantidade negativa, preço zero, UF inexistente, data no futuro | Barrado na validação |

A impressão digital é o detalhe que mais importa na prática: ela é calculada sobre data,
cliente, produto, quantidade, preço e UF — normalizados — e tem índice único no banco.
Reimportar o arquivo de ontem é uma operação segura.

---

## Como o arquivo está organizado

```
ampere.py
├── 1. Validação      regras de negócio em Pydantic: o que é uma venda válida
├── 2. Banco          schema SQLite e índices
├── 3. Consultas      SQL puro — toda agregação roda no banco
├── 4. Leitura        CSV: encoding, separador, apelidos de coluna
├── 5. API            rotas FastAPI
├── 6. Gerador        massa de dados de exemplo
├── 7. CLI            python ampere.py [rodar | dados]
└── 8. Painel         HTML, CSS e JS embutidos, zero dependência de front
```

Decisões que valem explicação:

- **SQLite com `sqlite3` puro, sem ORM.** O projeto roda com `git clone` e nada mais.
  O SQL fica à vista, que é o ponto do projeto.
- **Agregação no banco, não em Python.** `SUM`, `GROUP BY` e índices em `data`,
  `produto`, `categoria` e `uf`.
- **Painel sem framework.** Os gráficos são SVG gerados por JavaScript — sem
  `node_modules`, sem build.

---

## Endpoints

| Método | Rota | Para quê |
| --- | --- | --- |
| `POST` | `/api/v1/vendas/importar` | Envia o CSV e recebe o relatório de ingestão |
| `GET` | `/api/v1/vendas` | Lista com filtros, ordenação e paginação |
| `DELETE` | `/api/v1/vendas` | Zera a base |
| `GET` | `/api/v1/metricas/resumo` | Faturamento, pedidos, itens, ticket médio, clientes |
| `GET` | `/api/v1/metricas/produtos` | Ranking por faturamento |
| `GET` | `/api/v1/metricas/ufs` | Distribuição por estado |
| `GET` | `/api/v1/metricas/serie` | Série por dia ou por mês |
| `GET` | `/api/v1/metricas/crescimento` | Série mensal com variação contra o mês anterior |
| `GET` | `/health` | Checagem de saúde |

Filtros aceitos nas consultas: `inicio`, `fim`, `produto`, `categoria`, `uf`.

### Resposta da importação

```json
{
  "arquivo": "vendas_com_erros.csv",
  "linhas_lidas": 12,
  "gravadas": 5,
  "duplicadas": 0,
  "rejeitadas": 7,
  "taxa_aproveitamento": 41.7,
  "detalhe_rejeicoes": [
    {
      "linha": 4,
      "erros": ["data: Value error, data '32/03/2026' não está em um formato reconhecido"],
      "conteudo": {"data": "32/03/2026", "cliente": "Mecânica Pampa", "uf": "RS"}
    },
    {
      "linha": 7,
      "erros": ["preco_unitario: Input should be greater than 0"],
      "conteudo": {"produto": "Bateria 150Ah estacionária", "preco_unitario": "0"}
    }
  ]
}
```

---

## Configuração

| Variável | Padrão | Função |
| --- | --- | --- |
| `AMPERE_DB_PATH` | `data/ampere.db` | Caminho do banco |
| `AMPERE_LIMITE_UPLOAD_MB` | `10` | Tamanho máximo do CSV |

---

## Próximos passos

- [ ] Exportar o resultado filtrado em CSV e XLSX
- [ ] Autenticação por chave de API nas rotas de escrita
- [ ] Alerta de queda de faturamento por produto
- [ ] Trocar SQLite por PostgreSQL via variável de ambiente

---

Feito por **Diogo Dutra da Silva** — Ciência da Computação no CESUCA, Cachoeirinha/RS.
Licença MIT.
