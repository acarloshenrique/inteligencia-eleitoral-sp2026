# Lake Health Report

- Generated at: `2026-04-19T00:38:30.521511+00:00`
- Aggregate quality score: `0.796`
- Production ready: `4`
- Limited use: `7`
- Not ready: `3`

## Dataset Status

| Dataset | Score | Readiness | Rows | Limitations |
| --- | ---: | --- | ---: | --- |
| `gold_territorial_electoral_master_index` | 0.943 | production_ready | 103021 | Freshness timestamp not found. |
| `gold_candidate_context` | 0.739 | limited_use | 1 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_territory_profile` | 0.943 | production_ready | 103021 | Freshness timestamp not found. |
| `gold_electoral_base_strength` | 0.943 | production_ready | 103021 | Freshness timestamp not found. |
| `gold_competition_landscape` | 0.739 | limited_use | 103021 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_campaign_finance_efficiency` | 0.739 | limited_use | 1 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_thematic_affinity` | 0.739 | limited_use | 103021 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_priority_score` | 0.950 | production_ready | 103021 | Freshness timestamp not found. |
| `gold_allocation_inputs` | 0.738 | limited_use | 103021 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_allocation_recommendations` | 0.770 | limited_use | 103021 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_territorial_clusters` | 0.602 | not_ready | 779 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found.; No joinability keys available. |
| `gold_candidate_comparisons` | 0.739 | limited_use | 103021 | Freshness timestamp not found.; No territorial coverage columns found.; No temporal coverage column found. |
| `gold_zone_priority_score` | 0.784 | not_ready | 779 | Uniqueness on primary key: ['candidate_id'].; Freshness timestamp not found. |
| `gold_section_master_index_quality` | 0.784 | not_ready | 103021 | Uniqueness on primary key: ['ano_eleicao'].; Freshness timestamp not found. |

## Trusted Joins

- `gold_allocation_inputs:joinability`
- `gold_allocation_recommendations->gold_priority_score:candidate_id,territorio_id`
- `gold_allocation_recommendations:joinability`
- `gold_campaign_finance_efficiency:joinability`
- `gold_candidate_comparisons:joinability`
- `gold_candidate_context:joinability`
- `gold_competition_landscape:joinability`
- `gold_electoral_base_strength:joinability`
- `gold_priority_score->gold_territory_profile:territorio_id`
- `gold_priority_score:joinability`
- `gold_section_master_index_quality:joinability`
- `gold_territorial_electoral_master_index:joinability`
- `gold_territory_profile:joinability`
- `gold_thematic_affinity:joinability`
- `gold_zone_priority_score:joinability`

## Check Details

### gold_territorial_electoral_master_index

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['master_record_id', 'ano_eleicao', 'uf', 'cod_municipio_tse', 'zona', 'secao', 'join_confidence', 'source_coverage_score'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['master_record_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['ano_eleicao', 'uf', 'cod_municipio_tse', 'zona', 'secao', 'join_confidence'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **pass** score=1.000; observed=1.0; threshold=0.9; Territorial coverage across ['uf', 'cod_municipio_tse', 'cod_municipio_ibge', 'zona', 'secao'].
- `temporal_coverage` / `temporal_coverage`: **pass** score=1.000; observed=2024-2024; threshold=at least one valid year; Temporal coverage based on election year.
- `joinability` / `joinability_confidence`: **pass** score=0.998; observed=0.998357; threshold=0.85; Joinability based on join_confidence.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['join_confidence', 'source_coverage_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_candidate_context

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['candidate_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['candidate_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['candidate_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['territorios_cobertos', 'municipios_cobertos', 'zonas_cobertas', 'source_coverage_avg', 'join_confidence_avg'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_territory_profile

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['territorio_id', 'ano_eleicao', 'uf'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['territorio_id', 'ano_eleicao', 'uf'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **pass** score=1.000; observed=1.0; threshold=0.9; Territorial coverage across ['uf', 'cod_municipio_tse', 'cod_municipio_ibge', 'zona', 'secao'].
- `temporal_coverage` / `temporal_coverage`: **pass** score=1.000; observed=2024-2024; threshold=at least one valid year; Temporal coverage based on election year.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['cod_municipio_tse', 'cod_municipio_ibge', 'territorio_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['ano_eleicao', 'secoes', 'locais_votacao', 'candidatos', 'cd_setor_count', 'join_confidence_avg', 'source_coverage_avg', 'data_quality_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_electoral_base_strength

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['candidate_id', 'territorio_id', 'ano_eleicao', 'uf'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['candidate_id', 'territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id', 'territorio_id', 'ano_eleicao', 'uf'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **pass** score=1.000; observed=1.0; threshold=0.9; Territorial coverage across ['uf', 'cod_municipio_tse', 'zona', 'secao'].
- `temporal_coverage` / `temporal_coverage`: **pass** score=1.000; observed=2024-2024; threshold=at least one valid year; Temporal coverage based on election year.
- `joinability` / `joinability_confidence`: **pass** score=0.998; observed=0.998357; threshold=0.85; Joinability based on join_confidence.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['ano_eleicao', 'votos_nominais', 'total_aptos', 'source_coverage_score', 'join_confidence', 'base_strength_score', 'retention_score', 'expansion_signal'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_competition_landscape

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['territorio_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['territorio_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['territorio_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['candidate_count', 'leader_margin_score', 'competition_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_campaign_finance_efficiency

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['candidate_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['candidate_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['candidate_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['votos_nominais', 'receita_total', 'despesa_total', 'custo_por_voto_estimado', 'finance_efficiency_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_thematic_affinity

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['territorio_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['territorio_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['territorio_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['thematic_affinity_score', 'theme_evidence_count'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_priority_score

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['candidate_id', 'territorio_id', 'score_prioridade_final'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['candidate_id', 'territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id', 'territorio_id', 'score_prioridade_final'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **pass** score=1.000; observed=1.0; threshold=0.9; Territorial coverage across ['uf', 'cod_municipio_tse', 'zona', 'secao'].
- `temporal_coverage` / `temporal_coverage`: **pass** score=1.000; observed=2024-2024; threshold=at least one valid year; Temporal coverage based on election year.
- `joinability` / `joinability_confidence`: **pass** score=0.998; observed=0.998357; threshold=0.85; Joinability based on join_confidence.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['score_prioridade_final', 'base_strength_score', 'competition_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.
- `referential_integrity` / `referential_integrity`: **pass** score=1.000; observed=1.0; threshold=0.95; Referential integrity on keys: ['territorio_id'].

### gold_allocation_inputs

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['candidate_id', 'territorio_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['candidate_id', 'territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id', 'territorio_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_confidence`: **pass** score=0.998; observed=0.998357; threshold=0.85; Joinability based on join_confidence.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['score_prioridade_final', 'allocation_weight', 'data_quality_score', 'join_confidence'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_allocation_recommendations

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['scenario_id', 'candidate_id', 'territorio_id', 'recurso_sugerido'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['scenario_id', 'candidate_id', 'territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id', 'territorio_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['candidate_id', 'territorio_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['recurso_sugerido', 'percentual_orcamento_sugerido', 'score_prioridade_final'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.
- `referential_integrity` / `referential_integrity`: **pass** score=1.000; observed=1.0; threshold=0.95; Referential integrity on keys: ['candidate_id', 'territorio_id'].

### gold_territorial_clusters

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['territorial_cluster_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['territorial_cluster_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['territorial_cluster_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **fail** score=0.000; observed=None; threshold=None; No joinability keys available.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['territorios', 'priority_avg', 'secoes', 'locais_votacao'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_candidate_comparisons

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['territorio_id'].
- `uniqueness` / `uniqueness_primary_key`: **pass** score=1.000; observed=1.0; threshold=1.0; Uniqueness on primary key: ['territorio_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['territorio_id'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **fail** score=0.000; observed=None; threshold=None; No territorial coverage columns found.
- `temporal_coverage` / `temporal_coverage`: **fail** score=0.000; observed=None; threshold=None; No temporal coverage column found.
- `joinability` / `joinability_keys`: **pass** score=1.000; observed=1.0; threshold=0.85; Joinability based on keys: ['territorio_id'].
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['leader_margin_score', 'candidate_count', 'avg_priority_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_zone_priority_score

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['candidate_id', 'ano_eleicao', 'uf'].
- `uniqueness` / `uniqueness_primary_key`: **fail** score=0.001; observed=0.001284; threshold=1.0; Uniqueness on primary key: ['candidate_id'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['candidate_id', 'ano_eleicao', 'uf'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **pass** score=1.000; observed=1.0; threshold=0.9; Territorial coverage across ['uf', 'cod_municipio_tse', 'cod_municipio_ibge', 'zona'].
- `temporal_coverage` / `temporal_coverage`: **pass** score=1.000; observed=2024-2024; threshold=at least one valid year; Temporal coverage based on election year.
- `joinability` / `joinability_confidence`: **pass** score=0.998; observed=0.998198; threshold=0.85; Joinability based on join_confidence.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['ano_eleicao', 'territorios', 'secoes', 'locais_votacao', 'score_prioridade_final', 'score_disputabilidade', 'margem_estimada', 'base_eleitoral_score', 'potencial_expansao_score', 'custo_eficiencia_score', 'join_confidence', 'data_quality_score', 'source_coverage_score', 'confidence_score'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.

### gold_section_master_index_quality

- `completeness` / `completeness_required_columns`: **pass** score=1.000; observed=1.0; threshold=0.95; Completeness across required columns: ['ano_eleicao', 'uf'].
- `uniqueness` / `uniqueness_primary_key`: **fail** score=0.000; observed=1e-05; threshold=1.0; Uniqueness on primary key: ['ano_eleicao'].
- `validity` / `validity_keys`: **pass** score=1.000; observed=1.0; threshold=0.98; Validity for key columns: ['ano_eleicao', 'uf'].
- `freshness` / `freshness_metadata`: **warn** score=0.500; observed=missing_timestamp; threshold=30; Freshness timestamp not found.
- `territorial_coverage` / `territorial_coverage`: **pass** score=1.000; observed=1.0; threshold=0.9; Territorial coverage across ['uf', 'cod_municipio_tse', 'cod_municipio_ibge', 'zona', 'secao'].
- `temporal_coverage` / `temporal_coverage`: **pass** score=1.000; observed=2024-2024; threshold=at least one valid year; Temporal coverage based on election year.
- `joinability` / `joinability_confidence`: **pass** score=0.998; observed=0.998357; threshold=0.85; Joinability based on join_confidence.
- `distribution` / `distribution_numeric`: **pass** score=1.000; observed=1.0; threshold=1.0; Distribution sanity for numeric columns: ['ano_eleicao', 'registros', 'candidatos', 'setores_censitarios', 'join_confidence', 'source_coverage_score', 'section_quality_score', 'join_is_exact', 'join_is_approximate'].
- `drift` / `drift_numeric_mean`: **pass** score=1.000; observed=baseline_missing; threshold=0.2; No previous baseline available; drift check passes as initial run.
