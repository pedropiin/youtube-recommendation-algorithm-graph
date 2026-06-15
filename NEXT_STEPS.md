# Próximos Passos — MC859

## Status atual
- **Coleta**: crawl completo (grafos por dia: 31/03, 01/04, 02/04, 03/04 + grafo full)
- **Classificação**: 8.420 vídeos classificados via OpenAI → `classifications_openai.jsonl`
- **Plots existentes**: distribuição de graus (in/out), distribuição de SCCs, distribuição de labels

---

## Passo 1 — Anotar o grafo com as classificações

**O que fazer:** carregar o grafo (networkx a partir dos JSONs de crawl) e adicionar o
atributo `label` a cada nó, lendo de `classifications_openai.jsonl`.

**Como implementar:**
- Criar um dict `{video_id: openai_label}` a partir do JSONL
- Em `build_graph.py` (ou script separado), iterar sobre `G.nodes` e fazer
  `G.nodes[vid]['label'] = label_map.get(vid, 'desconhecido')`
- Exportar o grafo anotado como `.gexf` para uso nos passos seguintes
- Verificar cobertura: quantos nós do grafo têm label vs. total de nós

---

## Passo 2 — Análise de caminhos até conteúdo extremista

**O que fazer:** para cada vídeo **seed** (primeiro vídeo clicado em cada corrida do crawl —
recomendação da homepage), calcular o menor caminho até qualquer vídeo extremista.

**Métricas alvo (do projeto):**
- Distância mínima de cada seed até o extremista mais próximo alcançável
- Distribuição dessas distâncias mínimas sobre todos os seeds
- Fração de seeds que conseguem alcançar algum extremista (e com qual profundidade)
- Distância média (excluindo seeds sem caminho)

**Como implementar:**
- Identificar os nós seed: são os vídeos com `depth == 0` (ou equivalente) nos JSONs de crawl
- Definir conjunto de nós "extremistas": labels `teoria da conspiração ou desinformação` e
  `conteúdo politicamente polarizador` (e `extremismo ou radicalização` se existir)
- Para cada seed: `nx.single_source_shortest_path_length(G, seed)`, filtrar pelos nós
  extremistas alcançáveis, pegar o mínimo
- Plotar histograma das distâncias mínimas; reportar média e fração de alcançabilidade

---

## Passo 3 — Análise de caminhos contaminantes

**O que fazer:** identificar e quantificar caminhos que partem de conteúdo não-extremista
e chegam a conteúdo extremista (os "contaminating paths" do projeto).

**Como implementar:**
- Um caminho contaminante é qualquer aresta (u→v) onde `label[u]` é neutro e `label[v]`
  é extremista — ou, mais amplo, qualquer caminho simples neutro→extremista de comprimento k
- Contar quantas arestas cruzam de não-extremista para extremista
- Para os caminhos maiores: BFS/DFS limitado em profundidade 5–10, registrar rotas
- Plotar: histograma de comprimentos desses caminhos; quais categorias aparecem logo
  antes de um nó extremista (os "gateways")

---

## Passo 4 — Análise de SCCs e sumidouros por label

**O que fazer:** já temos as SCCs calculadas; agora cruzar com labels para saber quais
tipos de conteúdo formam clusters fechados (sumidouros).

**Como implementar:**
- Para cada SCC com tamanho > 1, calcular a distribuição de labels dos seus vértices
- Identificar SCCs onde a label majoritária é extremista → esses são os "sumidouros
  radicalizadores"
- Verificar se essas SCCs têm grau de saída baixo (poucos vizinhos externos) → confirma
  comportamento de sumidouro
- Plotar: top-N SCCs por tamanho, coloridas por label majoritária

---

## Passo 5 — Assimetria de recomendação entre categorias

**O que fazer:** construir uma matriz de transição de labels: dado que estou em um vídeo
de categoria X, qual a probabilidade de a próxima recomendação ser categoria Y?

**Como implementar:**
- Percorrer todas as arestas do grafo; para cada aresta (u→v), registrar `(label[u], label[v])`
- Montar matriz `N_labels × N_labels` com contagens, normalizar por linha → probabilidade
- Visualizar como heatmap (seaborn ou matplotlib)
- Destacar: quão frequente é a transição qualquer_label → extremista?

---

## Passo 6 — Vídeos mais frequentemente recomendados (hubs)

**O que fazer:** identificar os vídeos com maior in-degree (mais vezes recomendados) e
cruzar com suas labels.

**Como implementar:**
- `sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:50]`
- Para cada top-N, mostrar: título, label, in-degree, out-degree
- Plotar: top-20 vídeos por in-degree, coloridos por label
- Analisar: há hubs de conteúdo extremista? São mais "alcançados" do que esperado?

---

## Passo 7 — Comparação entre tempos de visualização (5s / 30s / 60s)

**O que fazer:** repetir as métricas dos Passos 2–6 separadamente para os grafos de
cada tempo de visualização e comparar.

**Como implementar:**
- Os grafos por data correspondem a runs diferentes; verificar se cada dia tem um tempo
  de visualização específico (confirmar com os JSONs de crawl)
- Gerar tabela comparativa: nós totais, arestas, fração de conteúdo extremista, distância
  média até extremista, para cada condição experimental
- Se os dados de tempo de visualização não estiverem separados nos JSONs, isso pode
  não ser possível sem nova coleta

---

## Passo 8 — Validação manual de amostra

**O que fazer:** o projeto pede validação manual de uma amostra aleatória das
classificações (já que são aproximações).

**Como implementar:**
- Sortear ~50–100 vídeos aleatoriamente (estratificado por label)
- Criar uma planilha simples com: video_id, title, label_openai, label_manual
- Calcular accuracy / concordância entre label automática e manual
- Reportar isso na análise como limitação/validação do método

---

## Passo 9 — Plots e visualizações finais

Plots que ainda precisamos (além dos que já existem):

| Plot | Dados necessários |
|------|------------------|
| Heatmap de transição entre labels | Grafo anotado (Passo 5) |
| Histograma de distâncias seed→extremista | Passo 2 |
| Top-20 hubs por label (bar chart) | Passo 6 |
| SCCs coloridas por label majoritária | Passo 4 |
| Comparativo entre condições experimentais | Passo 7 |
| Distribuição de labels (já existe) | `classifications_openai.jsonl` |

---

## Ideias adicionais / melhorias

- **PageRank por label**: calcular PageRank dos nós extremistas — se for alto, significa
  que o algoritmo os privilegia mesmo indiretamente
- **Betweenness centrality dos "gateways"**: os vídeos que aparecem logo antes de
  conteúdo extremista nos caminhos contaminantes — quão centrais são na rede?
- **Análise temporal**: comparar grafo do dia 31/03 vs. 03/04 — o algoritmo muda ao
  longo do tempo / ao longo das iterações?
- **Grafo de labels agregado**: colapsar todos os nós por label e criar um meta-grafo
  com pesos nas arestas → visualização macro do fluxo de recomendações
- **Probabilidade de atingir extremista via random walk ponderado**: dado um seed,
  simular (ou calcular analiticamente via matriz de transição estocástica) a probabilidade
  de chegar a um nó extremista em um random walk que usa os pesos das arestas como
  probabilidades de transição. A probabilidade de seguir a aresta (u→v) seria
  `weight(u,v) / sum(weight(u,*))`. Isso complementa o Passo 2 (que usa BFS/hops)
  com uma métrica que respeita a frequência observada de cada recomendação.

---

## Ordem de prioridade sugerida

1. Passo 1 (anotar grafo) — bloqueante para quase tudo
2. Passo 5 (matriz de transição) — resultado visual forte, rápido de implementar
3. Passo 6 (hubs) — rápido, resultado direto
4. Passo 2 + 3 (caminhos) — análise central do projeto
5. Passo 4 (SCCs + sumidouros)
6. Passo 7 (comparação temporal/experimental)
7. Passo 8 (validação manual)
8. Passo 9 (plots finais consolidados)
