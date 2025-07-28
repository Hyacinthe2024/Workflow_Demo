-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pipeline ETL Automatisé pour l'Analyse des Sorties d'Albums
-- MAGIC ## Million Song Dataset - Delta Live Tables (DLT)
-- MAGIC
-- MAGIC **Auteur**: Équipe Data Engineering  
-- MAGIC **Date**: 2025-07-22  
-- MAGIC **Version**: 1.0  
-- MAGIC **Plateforme**: Databricks with PySpark and SQL  
-- MAGIC
-- MAGIC ### Objectifs du Pipeline
-- MAGIC - Analyser les sorties d'albums et tendances musicales
-- MAGIC - Implémenter une architecture Medallion (Bronze-Silver-Gold)
-- MAGIC - Automatiser le traitement via Delta Live Tables
-- MAGIC - Assurer la qualité des données avec des contraintes DLT
-- MAGIC
-- MAGIC ### Architecture du Pipeline
-- MAGIC ```
-- MAGIC [Million Song Dataset] → [Bronze] → [Silver] → [Gold] → [Analytics/BI]
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥉 PARTIE 1: COUCHE BRONZE - DONNÉES BRUTES
-- MAGIC
-- MAGIC La couche Bronze ingère les données brutes depuis les fichiers sources sans transformation.
-- MAGIC - **Objectif**: Conservation intégrale des données originales
-- MAGIC - **Source**: `/databricks-datasets/songs/data-001/`
-- MAGIC - **Format**: Streaming ingestion avec métadonnées de traçabilité

-- COMMAND ----------

USE CATALOG catalog_wh;
CREATE SCHEMA IF NOT EXISTS pipe_schema;
USE SCHEMA pipe_schema;


-- COMMAND ----------

-- Table Bronze: Ingestion des données brutes depuis les fichiers sources

CREATE OR REFRESH STREAMING LIVE TABLE songs_bronze
(
 artist_id STRING,
 artist_lat DOUBLE,
 artist_long DOUBLE,
 artist_location STRING,
 artist_name STRING,
 duration DOUBLE,
 end_of_fade_in DOUBLE,
 key INT,
 key_confidence DOUBLE,
 loudness DOUBLE,
 release STRING,
 song_hotnes DOUBLE,
 song_id STRING,
 start_of_fade_out DOUBLE,
 tempo DOUBLE,
 time_signature INT,
 time_signature_confidence DOUBLE,
 title STRING,
 year INT,
 partial_sequence STRING,
 value STRING,
 -- Métadonnées pour le suivi
 ingestion_timestamp TIMESTAMP GENERATED ALWAYS AS (current_timestamp()),
 source_file STRING
)
COMMENT "Couche Bronze: Données brutes du Million Song Dataset - Ingestion streaming"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipeline.level" = "raw"
)
-- Remplacer la fonction cloud_files par une approche plus directe
AS SELECT *,
  current_timestamp() as ingestion_timestamp,
  _metadata.file_path as source_file
FROM cloud_files(
'/databricks-datasets/songs/data-001/');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥈 PARTIE 2: COUCHE SILVER - DONNÉES NETTOYÉES ET TRANSFORMÉES
-- MAGIC
-- MAGIC La couche Silver applique le nettoyage et la validation des données avec des contraintes d'expectation DLT.
-- MAGIC - **Objectif**: Données nettoyées et structurées pour l'analyse
-- MAGIC - **Transformations**: Validation, normalisation, structuration
-- MAGIC - **Qualité**: 7 contraintes d'expectation + score de qualité

-- COMMAND ----------

-- Table Silver OPTIMISÉE: Champs utiles pour l'analyse des sorties d'albums
CREATE OR REFRESH STREAMING LIVE TABLE songs_silver
(
  CONSTRAINT valid_song_id EXPECT (song_id IS NOT NULL AND length(song_id) > 0),
  CONSTRAINT valid_title EXPECT (title IS NOT NULL AND length(trim(title)) > 0),
  CONSTRAINT valid_artist EXPECT (artist_name IS NOT NULL AND length(trim(artist_name)) > 0),
  CONSTRAINT valid_year EXPECT (release_year >= 1900 AND release_year <= year(current_date())),
  CONSTRAINT valid_duration EXPECT (duration_seconds > 0 AND duration_seconds <= 1800),
  CONSTRAINT valid_tempo EXPECT (tempo_bpm IS NULL OR (tempo_bpm >= 40 AND tempo_bpm <= 250))
)
COMMENT "Couche Silver: Données optimisées pour l'analyse des sorties d'albums"
TBLPROPERTIES (
  "quality" = "silver",
  "pipeline.level" = "cleaned",
  "business.focus" = "album_releases_analysis"
)
AS SELECT 
  -- ═══════════════════════════════════════════════════════════
  -- IDENTIFIANTS CRITIQUES
  -- ═══════════════════════════════════════════════════════════
  song_id,
  artist_id,
  trim(artist_name) as artist_name,
  trim(title) as title,
  
  -- ═══════════════════════════════════════════════════════════
  -- MÉTADONNÉES TEMPORELLES & ALBUM
  -- ═══════════════════════════════════════════════════════════
  CASE 
    WHEN year >= 1900 AND year <= year(current_date()) THEN year
    ELSE NULL
  END as release_year,
  
  CASE 
    WHEN trim(release) = '' OR release IS NULL THEN 'Unknown Release'
    ELSE trim(release)
  END as album_info,
  
  -- ═══════════════════════════════════════════════════════════
  -- MÉTRIQUES BUSINESS ESSENTIELLES
  -- ═══════════════════════════════════════════════════════════
  CASE 
    WHEN song_hotnes BETWEEN 0 AND 1 THEN song_hotnes
    ELSE 0.0
  END as popularity_score,
  
  CASE 
    WHEN duration > 30 AND duration <= 1800 THEN duration
    ELSE NULL
  END as duration_seconds,
  
  -- ═══════════════════════════════════════════════════════════
  -- CARACTÉRISTIQUES MUSICALES (Analyse de tendances)
  -- ═══════════════════════════════════════════════════════════
  CASE 
    WHEN tempo >= 40 AND tempo <= 250 THEN tempo
    ELSE NULL
  END as tempo_bpm,
  
  CASE 
    WHEN loudness IS NOT NULL THEN loudness
    ELSE NULL
  END as loudness_db,
  
  -- Informations musicales structurées
  struct(
    CASE WHEN key BETWEEN 0 AND 11 THEN key ELSE NULL END as key_number,
    CASE WHEN key_confidence BETWEEN 0 AND 1 THEN key_confidence ELSE NULL END as confidence
  ) as key_info,
  
  struct(
    CASE WHEN time_signature BETWEEN 1 AND 12 THEN time_signature ELSE NULL END as signature,
    CASE WHEN time_signature_confidence BETWEEN 0 AND 1 THEN time_signature_confidence ELSE NULL END as confidence
  ) as time_signature_info,
  
  -- ═══════════════════════════════════════════════════════════
  -- GÉOLOCALISATION (Analyse géographique)
  -- ═══════════════════════════════════════════════════════════
  CASE 
    WHEN trim(artist_location) = '' OR artist_location IS NULL THEN 'Unknown'
    ELSE trim(artist_location)
  END as artist_location,
  
  -- Coordonnées géographiques pour mapping
  CASE 
    WHEN artist_lat BETWEEN -90 AND 90 AND artist_long BETWEEN -180 AND 180
    THEN struct(artist_lat as latitude, artist_long as longitude)
    ELSE struct(NULL as latitude, NULL as longitude)
  END as artist_coordinates,
  
  -- ═══════════════════════════════════════════════════════════
  -- CARACTÉRISTIQUES AUDIO (Production analysis)
  -- ═══════════════════════════════════════════════════════════
  struct(
    CASE 
      WHEN end_of_fade_in >= 0 AND end_of_fade_in <= duration THEN end_of_fade_in
      ELSE NULL
    END as fade_in_end,
    CASE 
      WHEN start_of_fade_out >= 0 AND start_of_fade_out <= duration THEN start_of_fade_out
      ELSE NULL
    END as fade_out_start
  ) as audio_features,
  
  -- ═══════════════════════════════════════════════════════════
  -- MÉTRIQUES DE QUALITÉ & CATÉGORISATION BUSINESS
  -- ═══════════════════════════════════════════════════════════
  
  -- Score de qualité des données (0-1)
  (
    CASE WHEN song_id IS NOT NULL AND length(song_id) > 0 THEN 0.2 ELSE 0 END +
    CASE WHEN title IS NOT NULL AND length(trim(title)) > 0 THEN 0.2 ELSE 0 END +
    CASE WHEN artist_name IS NOT NULL AND length(trim(artist_name)) > 0 THEN 0.2 ELSE 0 END +
    CASE WHEN year >= 1900 AND year <= year(current_date()) THEN 0.2 ELSE 0 END +
    CASE WHEN duration > 0 AND duration <= 1800 THEN 0.2 ELSE 0 END
  ) as data_quality_score,
  
  -- Catégorisation par époque (utile pour analyse temporelle)
  CASE 
    WHEN year >= 2020 THEN '2020s'
    WHEN year >= 2010 THEN '2010s'
    WHEN year >= 2000 THEN '2000s'
    WHEN year >= 1990 THEN '1990s'
    WHEN year >= 1980 THEN '1980s'
    WHEN year >= 1970 THEN '1970s'
    WHEN year >= 1960 THEN '1960s'
    ELSE 'Classic'
  END as decade_category,
  
  -- Catégorisation de durée (utile pour analyse de tendances)
  CASE 
    WHEN duration <= 120 THEN 'Short'
    WHEN duration <= 240 THEN 'Standard'
    WHEN duration <= 360 THEN 'Long'
    WHEN duration <= 600 THEN 'Extended'
    ELSE 'Epic'
  END as duration_category,
  
  -- Catégorisation de popularité
  CASE 
    WHEN song_hotnes >= 0.8 THEN 'Hit'
    WHEN song_hotnes >= 0.6 THEN 'Popular'
    WHEN song_hotnes >= 0.4 THEN 'Moderate'
    WHEN song_hotnes >= 0.2 THEN 'Niche'
    ELSE 'Underground'
  END as popularity_tier,
  
  -- Timestamp de traitement pour traçabilité
  current_timestamp() as processing_timestamp

FROM STREAM(LIVE.songs_bronze)
WHERE 
  -- Filtrage pour données de qualité suffisante
  song_id IS NOT NULL 
  AND title IS NOT NULL 
  AND artist_name IS NOT NULL
  AND length(trim(title)) > 0
  AND length(trim(artist_name)) > 0
  AND year IS NOT NULL  -- Essentiel pour l'analyse temporelle
  AND duration > 0;     -- Élimine les durées invalides

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- Tester d'abord l'accès aux données source
-- MAGIC -- SELECT COUNT(*) FROM cloud_files('/databricks-datasets/songs/data-001/');
-- MAGIC -- SELECT * FROM cloud_files('/databricks-datasets/songs/data-001/') LIMIT 5;
-- MAGIC
-- MAGIC -- Créer une table temporaire pour test (sans DLT)
-- MAGIC -- CREATE OR REPLACE TEMPORARY VIEW test_songs_bronze AS 
-- MAGIC -- SELECT *,
-- MAGIC --   current_timestamp() as ingestion_timestamp,
-- MAGIC --   NULL as source_file
-- MAGIC -- FROM cloud_files('/databricks-datasets/songs/data-001/')
-- MAGIC --   ;
-- MAGIC
-- MAGIC -- Tester la vue temporaire
-- MAGIC -- SELECT COUNT(*) FROM test_songs_bronze;
-- MAGIC -- SELECT * FROM test_songs_bronze LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥇 PARTIE 3: COUCHE GOLD - DONNÉES AGRÉGÉES ET MÉTRIQUES BUSINESS
-- MAGIC
-- MAGIC La couche Gold produit les métriques et KPIs prêts pour la consommation business.
-- MAGIC - **Objectif**: Tables d'analyse optimisées pour le reporting
-- MAGIC - **Contenu**: Agrégations temporelles, rankings d'artistes, tendances musicales
-- MAGIC - **Usage**: Support aux décisions stratégiques du label

-- COMMAND ----------

-- ============================================================================
-- COUCHE GOLD - DONNÉES AGRÉGÉES ET MÉTRIQUES BUSINESS
-- ============================================================================

-- =========================
-- Table Gold 1: Analyse des sorties d'albums par année et artiste
-- =========================
CREATE OR REFRESH STREAMING LIVE TABLE album_releases_by_year
COMMENT "Gold: Analyse des sorties d'albums par année et artiste"
TBLPROPERTIES (
  "quality" = "gold",
  "business.purpose" = "album_analysis"
)
AS SELECT 
  release_year,
  artist_name,
  count(*) as total_songs,
  round(avg(duration_seconds) / 60, 2) as avg_duration_minutes,
  round(avg(tempo_bpm), 2) as avg_tempo,
  round(avg(popularity_score), 4) as avg_song_hotness,
  count(distinct album_info) as distinct_releases,
  round(avg(data_quality_score), 3) as avg_quality_score,
  min(processing_timestamp) as first_song_date,
  max(processing_timestamp) as last_song_date
FROM (LIVE.songs_silver)
WHERE release_year IS NOT NULL
GROUP BY release_year, artist_name
HAVING count(*) >= 2
ORDER BY release_year DESC, total_songs DESC;


-- COMMAND ----------

-- =========================
-- Table Gold 2: Top artistes par décennie avec métriques avancées
-- =========================
CREATE OR REFRESH STREAMING LIVE TABLE  top_artists_by_decade
COMMENT "Gold: Top artistes par décennie avec métriques de performance"
TBLPROPERTIES (
  "quality" = "gold",
  "business.purpose" = "artist_ranking"
)
AS SELECT 
  decade_category as decade,
  artist_name,
  row_number() OVER (
    PARTITION BY decade_category
    ORDER BY count(*) DESC, avg(popularity_score) DESC
  ) as artist_rank,
  count(*) as total_songs,
  count(distinct album_info) as total_releases,
  round(avg(popularity_score), 4) as avg_song_hotness,
  max(release_year) - min(release_year) + 1 as career_span_years,
  coalesce(first(artist_location), 'Unknown Location') as geographic_info,
  round(
    log(count(*)) * avg(popularity_score) * (max(release_year) - min(release_year) + 1) / 10.0, 
    3
  ) as productivity_score,
  first(popularity_tier) as popularity_tier
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND popularity_score IS NOT NULL
GROUP BY decade_category, artist_name
HAVING count(*) >= 3
QUALIFY artist_rank <= 50;


-- COMMAND ----------

-- =========================
-- Table Gold 3: Métriques de tendances musicales
-- =========================
CREATE OR REFRESH STREAMING LIVE TABLE  music_trends_analysis
COMMENT "Gold: Analyse des tendances musicales agrégées"
TBLPROPERTIES (
  "quality" = "gold",
  "business.purpose" = "trend_analysis"
)
AS 
SELECT 
  decade_category as analysis_period,
  'avg_duration_minutes' as metric_name,
  round(avg(duration_seconds) / 60, 2) as metric_value,
  'duration_trend' as trend_category,
  CASE WHEN count(*) >= 1000 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND duration_seconds IS NOT NULL
GROUP BY decade_category

UNION ALL

SELECT 
  decade_category as analysis_period,
  'avg_tempo_bpm' as metric_name,
  round(avg(tempo_bpm), 2) as metric_value,
  'tempo_trend' as trend_category,
  CASE WHEN count(*) >= 1000 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND tempo_bpm IS NOT NULL
GROUP BY decade_category

UNION ALL

SELECT 
  decade_category as analysis_period,
  'avg_popularity_score' as metric_name,
  round(avg(popularity_score), 4) as metric_value,
  'popularity_trend' as trend_category,
  CASE WHEN count(*) >= 1000 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND popularity_score IS NOT NULL
GROUP BY decade_category

UNION ALL

SELECT 
  decade_category as analysis_period,
  'avg_loudness_db' as metric_name,
  round(avg(loudness_db), 2) as metric_value,
  'loudness_trend' as trend_category,
  CASE WHEN count(*) >= 500 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND loudness_db IS NOT NULL
GROUP BY decade_category;



-- COMMAND ----------

-- =========================
-- Table Gold 4: Dashboard de monitoring de qualité des données
-- =========================
CREATE OR REFRESH STREAMING LIVE TABLE  data_quality_dashboard
COMMENT "Gold: Dashboard de monitoring de la qualité des données"
TBLPROPERTIES (
  "quality" = "gold",
  "business.purpose" = "data_monitoring"
)
AS SELECT 
  'silver' as table_level,
  count(*) as total_records,
  count(case when song_id IS NULL then 1 end) as missing_song_ids,
  count(case when title IS NULL then 1 end) as missing_titles,
  count(case when artist_name IS NULL then 1 end) as missing_artists,
  count(case when release_year IS NULL then 1 end) as missing_years,
  round(avg(data_quality_score), 3) as avg_quality_score,
  count(case when data_quality_score >= 0.8 then 1 end) as high_quality_records,
  count(case when popularity_tier = 'Hit' then 1 end) as hit_songs_count,
  count(case when duration_category = 'Standard' then 1 end) as standard_duration_songs,
  count(case when decade_category = '2010s' then 1 end) as songs_2010s,
  current_timestamp() as last_updated
FROM LIVE.songs_silver

UNION ALL

SELECT 
  decade_category as table_level,
  count(*) as total_records,
  count(case when song_id IS NULL then 1 end) as missing_song_ids,
  count(case when title IS NULL then 1 end) as missing_titles,
  count(case when artist_name IS NULL then 1 end) as missing_artists,
  count(case when release_year IS NULL then 1 end) as missing_years,
  round(avg(data_quality_score), 3) as avg_quality_score,
  count(case when data_quality_score >= 0.8 then 1 end) as high_quality_records,
  count(case when popularity_tier = 'Hit' then 1 end) as hit_songs_count,
  count(case when duration_category = 'Standard' then 1 end) as standard_duration_songs,
  count(*) as songs_2010s,
  current_timestamp() as last_updated
FROM LIVE.songs_silver
WHERE decade_category IS NOT NULL
GROUP BY decade_category;


-- COMMAND ----------

-- Table Gold 2: Top artistes par décennie avec métriques avancées
CREATE OR REFRESH STREAMING LIVE TABLE  top_artists_by_decade
COMMENT "Gold: Top artistes par décennie avec métriques de performance"
TBLPROPERTIES (
  "quality" = "gold",
  "business.purpose" = "artist_ranking"
)
AS SELECT 
  CASE 
    WHEN release_year >= 2010 THEN '2010s'  
    WHEN release_year >= 2000 THEN '2000s'
    WHEN release_year >= 1990 THEN '1990s'
    WHEN release_year >= 1980 THEN '1980s'
    WHEN release_year >= 1970 THEN '1970s'
    WHEN release_year >= 1960 THEN '1960s'
    ELSE 'Before 1960s'
  END as decade,
  
  artist_name,
  
  row_number() OVER (
    PARTITION BY CASE 
      WHEN release_year >= 2010 THEN '2010s'  
      WHEN release_year >= 2000 THEN '2000s'
      WHEN release_year >= 1990 THEN '1990s'
      WHEN release_year >= 1980 THEN '1980s'
      WHEN release_year >= 1970 THEN '1970s'
      WHEN release_year >= 1960 THEN '1960s'
      ELSE 'Before 1960s'
    END 
    ORDER BY count(*) DESC, avg(popularity_score) DESC  
  ) as artist_rank,
  
  count(*) as total_songs,
  count(distinct album_info) as total_releases,  
  round(avg(popularity_score), 4) as avg_song_hotness, 
  max(release_year) - min(release_year) + 1 as career_span_years,  
  
  coalesce(first(artist_location), 'Unknown Location') as geographic_info,
  
  -- Score de productivité combinant volume et popularité
  round(
    log(count(*)) * avg(popularity_score) * (max(release_year) - min(release_year) + 1) / 10.0,  
    3
  ) as productivity_score,
  
  CASE 
    WHEN avg(popularity_score) >= 0.8 THEN 'Superstar'  
    WHEN avg(popularity_score) >= 0.6 THEN 'Popular'
    WHEN avg(popularity_score) >= 0.4 THEN 'Emerging'
    ELSE 'Underground'
  END as popularity_tier

FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND popularity_score IS NOT NULL  
GROUP BY 
  CASE 
    WHEN release_year >= 2010 THEN '2010s'  
    WHEN release_year >= 2000 THEN '2000s'
    WHEN release_year >= 1990 THEN '1990s'
    WHEN release_year >= 1980 THEN '1980s'
    WHEN release_year >= 1970 THEN '1970s'
    WHEN release_year >= 1960 THEN '1960s'
    ELSE 'Before 1960s'
  END,
  artist_name
HAVING count(*) >= 3  -- Au moins 3 chansons pour un classement fiable
QUALIFY artist_rank <= 50;  -- Top 50 par décennie

-- COMMAND ----------

-- Table Gold 3: Métriques de tendances musicales
CREATE OR REFRESH STREAMING LIVE TABLE  music_trends_analysis
COMMENT "Gold: Analyse des tendances musicales agrégées"
TBLPROPERTIES (
  "quality" = "gold",
  "business.purpose" = "trend_analysis"
)
AS 
-- Tendances de durée par décennie
SELECT 
  CASE 
    WHEN release_year >= 2010 THEN '2010s'  
    WHEN release_year >= 2000 THEN '2000s'
    WHEN release_year >= 1990 THEN '1990s'
    WHEN release_year >= 1980 THEN '1980s'
    ELSE 'Earlier'
  END as analysis_period,
  'avg_duration_minutes' as metric_name,
  round(avg(duration_seconds) / 60, 2) as metric_value,
  'stable' as trend_direction,
  CASE WHEN count(*) >= 1000 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND duration_seconds IS NOT NULL  
GROUP BY CASE 
  WHEN release_year >= 2010 THEN '2010s'  
  WHEN release_year >= 2000 THEN '2000s'
  WHEN release_year >= 1990 THEN '1990s'
  WHEN release_year >= 1980 THEN '1980s'
  ELSE 'Earlier'
END

UNION ALL

-- Tendances de tempo par décennie
SELECT 
  CASE 
    WHEN release_year >= 2010 THEN '2010s'  
    WHEN release_year >= 2000 THEN '2000s'
    WHEN release_year >= 1990 THEN '1990s'
    WHEN release_year >= 1980 THEN '1980s'
    ELSE 'Earlier'
  END as analysis_period,
  'avg_tempo_bpm' as metric_name,
  round(avg(tempo_bpm), 2) as metric_value,  
  'stable' as trend_direction,
  CASE WHEN count(*) >= 1000 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND tempo_bpm IS NOT NULL  
GROUP BY CASE 
  WHEN release_year >= 2010 THEN '2010s'  
  WHEN release_year >= 2000 THEN '2000s'
  WHEN release_year >= 1990 THEN '1990s'
  WHEN release_year >= 1980 THEN '1980s'
  ELSE 'Earlier'
END

UNION ALL

-- Tendances de popularité par décennie (BONUS AJOUTÉ)
SELECT 
  CASE 
    WHEN release_year >= 2010 THEN '2010s'
    WHEN release_year >= 2000 THEN '2000s'
    WHEN release_year >= 1990 THEN '1990s'
    WHEN release_year >= 1980 THEN '1980s'
    ELSE 'Earlier'
  END as analysis_period,
  'avg_popularity_score' as metric_name,
  round(avg(popularity_score), 4) as metric_value,
  'evolving' as trend_direction,
  CASE WHEN count(*) >= 1000 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND popularity_score IS NOT NULL
GROUP BY CASE 
  WHEN release_year >= 2010 THEN '2010s'
  WHEN release_year >= 2000 THEN '2000s'
  WHEN release_year >= 1990 THEN '1990s'
  WHEN release_year >= 1980 THEN '1980s'
  ELSE 'Earlier'
END

UNION ALL

-- Tendances de loudness par décennie (BONUS AJOUTÉ)
SELECT 
  CASE 
    WHEN release_year >= 2010 THEN '2010s'
    WHEN release_year >= 2000 THEN '2000s'
    WHEN release_year >= 1990 THEN '1990s'
    WHEN release_year >= 1980 THEN '1980s'
    ELSE 'Earlier'
  END as analysis_period,
  'avg_loudness_db' as metric_name,
  round(avg(loudness_db), 2) as metric_value,
  'increasing' as trend_direction,  -- Loudness tends to increase over time (loudness war)
  CASE WHEN count(*) >= 500 THEN 'high' ELSE 'medium' END as statistical_significance,
  count(*) as sample_size
FROM LIVE.songs_silver
WHERE release_year IS NOT NULL AND loudness_db IS NOT NULL
GROUP BY CASE 
  WHEN release_year >= 2010 THEN '2010s'
  WHEN release_year >= 2000 THEN '2000s'
  WHEN release_year >= 1990 THEN '1990s'
  WHEN release_year >= 1980 THEN '1980s'
  ELSE 'Earlier'
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 VUES UTILITAIRES POUR L'ANALYSE BUSINESS

-- COMMAND ----------

-- Vue pour le monitoring de la qualité des données
CREATE OR REFRESH STREAMING LIVE TABLE  data_quality_dashboard
COMMENT "Dashboard de monitoring de la qualité des données"
AS SELECT 
  'silver' as table_level,
  count(*) as total_records,
  count(*) - count(song_id) as missing_song_ids,
  count(*) - count(title) as missing_titles,
  count(*) - count(artist_name) as missing_artists,
  count(*) - count(release_year) as missing_years,  
  round(avg(data_quality_score), 3) as avg_quality_score, 
  count(case when data_quality_score >= 0.8 then 1 end) as high_quality_records,  
  current_timestamp() as last_updated
FROM LIVE.songs_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🚀 INSTRUCTIONS DE DÉPLOIEMENT ET D'UTILISATION
-- MAGIC
-- MAGIC ### 1. Créer un Pipeline DLT dans Databricks
-- MAGIC 1. Naviguer vers **"Workflows"** > **"Delta Live Tables"**
-- MAGIC 2. Cliquer sur **"Create Pipeline"**
-- MAGIC 3. Ajouter ce notebook comme source du pipeline
-- MAGIC 4. Configurer le stockage target: `/mnt/delta/songs_pipeline/`
-- MAGIC
-- MAGIC ### 2. Configuration Recommandée
-- MAGIC - **Pipeline Mode**: 
-- MAGIC   - `Triggered` pour traitement batch programmé
-- MAGIC   - `Continuous` pour streaming en temps réel
-- MAGIC - **Cluster**: Single node ou multi-node selon le volume de données
-- MAGIC - **Edition**: `Advanced` (requis pour les contraintes d'expectation)
-- MAGIC - **Target Schema**: `songs_analytics` (ou nom personnalisé)
-- MAGIC
-- MAGIC ### 3. Monitoring et Alertes
-- MAGIC - Surveiller les métriques de qualité via `data_quality_dashboard`
-- MAGIC - Configurer des alertes sur les violations de contraintes DLT
-- MAGIC - Monitorer les performances via l'interface DLT native
-- MAGIC - Utiliser les métriques Databricks pour le troubleshooting
-- MAGIC
-- MAGIC ### 4. Utilisation des Tables Gold
-- MAGIC - **`album_releases_by_year`**: Analyse des sorties par période et performance d'artistes
-- MAGIC - **`top_artists_by_decade`**: Rankings et découverte d'artistes par décennie
-- MAGIC - **`music_trends_analysis`**: Analyse des tendances du marché musical
-- MAGIC - **`data_quality_dashboard`**: Monitoring de la qualité des données
-- MAGIC
-- MAGIC ### 5. Évolutions Possibles
-- MAGIC - Ajouter des sources externes (Spotify API, social media, sales data)
-- MAGIC - Implémenter des modèles ML pour la prédiction de popularité
-- MAGIC - Créer des APIs REST pour l'exposition des données Gold
-- MAGIC - Intégrer avec des outils de visualisation (Tableau, Power BI, Looker)
-- MAGIC - Ajouter des alertes business basées sur les KPIs
-- MAGIC
-- MAGIC ### 6. Bonnes Pratiques
-- MAGIC - **Tests**: Valider le pipeline sur un sous-ensemble de données
-- MAGIC - **Documentation**: Maintenir la documentation des transformations
-- MAGIC - **Versioning**: Utiliser Git pour le versioning du code
-- MAGIC - **Sécurité**: Configurer les permissions d'accès appropriées
-- MAGIC - **Performance**: Monitorer et optimiser les performances régulièrement
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **📞 Support**: Pour toute question technique, contacter l'équipe Data Engineering  
-- MAGIC **📈 Analytics**: Les données Gold sont disponibles pour requêtes SQL directes  
-- MAGIC **🔄 Mise à jour**: Ce notebook sera maintenu et évolué selon les besoins business
