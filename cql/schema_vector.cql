-- ============================================================================
-- Webinar 2 — vector search additions (applied on top of cql/schema.cql).
--
-- Thesis on display: the SAME ScyllaDB that serves millisecond feature
-- point-reads also answers ANN similarity queries. Embeddings live next to
-- the features they're derived from; the vector index (external Vector Store
-- service, USearch/HNSW) is kept fresh from the base table via CDC — no second
-- database, no app-level sync pipeline.
--
-- Dimensions must match feature_store/embeddings.py (WALLET_DIM).
-- ============================================================================

USE feature_store;

-- ----------------------------------------------------------------------------
-- Per-wallet behaviour embedding, stored NEXT TO the wallet features.
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
-- Per (wallet, coin) behaviour embedding — the SAME 16-dim fingerprint, scoped
-- to one wallet's activity in one coin ("how does this wallet trade BTC?").
--
-- Why both: the wallet index answers "who trades like this wallet"; this one
-- answers "which wallet-coin books behave alike", and it is 4.4x larger —
-- 981k pairs vs 224k wallets on the 46-day dataset — so it is what gives the
-- index a realistic population. Dims 5/6 (coin diversity / top-coin share) are
-- constant here by construction; kept anyway so ONE embedding function serves
-- both scopes rather than forking the canonical code.
-- ----------------------------------------------------------------------------
ALTER TABLE wallet_coin_features ADD (embedding vector<float, 16>);

CREATE CUSTOM INDEX IF NOT EXISTS wallet_coin_embedding_idx
ON wallet_coin_features (embedding)
USING 'vector_index'
WITH OPTIONS = { 'similarity_function': 'COSINE' };
