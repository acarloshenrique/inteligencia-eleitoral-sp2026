# Catálogo de Fontes por Domínio

O lago está organizado em cinco domínios de fonte:

- `eleitoral_oficial`
- `socioeconomico`
- `territorial`
- `midia_e_social`
- `operacoes_de_campanha`

Objetivo:

- separar origem de sinal por finalidade analítica
- facilitar governança de aquisição
- permitir priorização comercial por domínio
- manter ingestão automatizada com contrato explícito

Arquivos de referência:

- `config/source_domains.json`: taxonomia oficial dos domínios
- `config/source_catalogs/*.json`: catálogo de fontes por domínio
- `config/ingestion_sources.example.json`: exemplo de catálogo de ingestão com agrupamento por domínio

Regras:

- todo asset de ingestão deve pertencer a um domínio
- o manifesto de ingestão publica `dominio_fonte` por asset
- todo download automatizado é persistido cru em `lake/bronze/<dominio>/fonte=<asset>/coleta=<timestamp>/`
- a orquestração publica resumo agregado em `dominios`
- catálogos legados com `assets` planos continuam aceitos; o domínio é inferido por `role` quando possível

Prioridade atual:

- `eleitoral_oficial` é a espinha dorsal do produto
- as fontes TSE entram primeiro no lago para atualização recorrente, auditável e reaproveitável
- cobertura prioritária inicial:
  - eleitorado por município
  - eleitorado por zona
  - eleitorado por seção
  - resultados históricos por município, zona e seção
  - candidaturas
  - prestações de contas
  - pesquisas eleitorais, quando disponíveis
  - comparecimento e abstenção
  - partidos, coligações e federações

Ampliação socioeconômica:

- `socioeconomico` usa IBGE como base nacional e Seade como complemento prioritário para SP
- cobertura útil inicial:
  - população
  - densidade
  - renda
  - escolaridade
  - urbanização
  - idade
  - emprego formal
  - acesso à internet
  - estrutura urbana
  - ruralidade
- uso analítico previsto:
  - reforçar `potencial_eleitoral_ajustado_social`
  - derivar `custo_mobilizacao_relativo`
