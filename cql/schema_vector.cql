-- ============================================================================
-- Webinar 2 — vector search additions (applied on top of cql/schema.cql).
--
-- Thesis on display: the SAME ScyllaDB that serves millisecond feature
-- point-reads also answers ANN similarity queries. Embeddings live next to
-- the features they're derived from; the vector index (external Vector Store
-- service, USearch/HNSW) is kept fresh from the base table via CDC — no second
-- database, no app-level sync pipeline.
--
-- Dimensions must match feature_store/embeddings.py (WALLET_DIM / COIN_DIM).
-- ============================================================================

USE feature_store;

-- ----------------------------------------------------------------------------
-- 1) Per-wallet behaviour embedding, stored NEXT TO the wallet features.
--    Hand-crafted + normalized (explainable on stage): directionality, maker
--    share, taker-buy bias, activity scale, coin diversity/concentration,
--    PnL profile, archetype, hours-of-day profile. Similar wallets — similar
--    behaviour — sit near each other in this space.
-- ----------------------------------------------------------------------------
ALTER TABLE wallet_features ADD (embedding vector<float, 16>);

-- Global ANN index: the headline query is "find wallets like this one across
-- the WHOLE population" (copy-trade + ring detection), so a global index is
-- the right scope. COSINE because component scales differ after squashing.
-- NOTE: vector index options can't be ALTERed — drop + recreate to retune.
CREATE CUSTOM INDEX IF NOT EXISTS wallet_embedding_idx
ON wallet_features (embedding)
USING 'vector_index'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

-- ----------------------------------------------------------------------------
-- 2) Per-coin flow-signature embedding: rolling
--    [volume, imbalance, hhi, smart_flow, active_wallets] x [1m, 5m, 1h].
--    One row per coin (live overwrite) -> "which coins look like COIN right
--    now" is one ANN query over a small, always-fresh table.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coin_flow_vectors (
    coin        text PRIMARY KEY,
    embedding   vector<float, 15>,
    updated_ts  timestamp
) WITH compaction = {'class': 'IncrementalCompactionStrategy'};

CREATE CUSTOM INDEX IF NOT EXISTS coin_flow_embedding_idx
ON coin_flow_vectors (embedding)
USING 'vector_index'
WITH OPTIONS = { 'similarity_function': 'COSINE' };
