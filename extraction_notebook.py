# Databricks notebook source
# MAGIC %md
# MAGIC # Extraction du Dataset Databricks Songs vers JSON
# MAGIC ## Convertir les données Parquet vers JSON dans votre volume
# MAGIC
# MAGIC **Volume de destination**: `/Volumes/dbacademy/database_ops/labuser_volume`  
# MAGIC **Source**: `/databricks-datasets/songs/data-001/`  
# MAGIC **Format**: Parquet → JSON  
# MAGIC **Date**: 2025-07-30
# MAGIC
# MAGIC ### Objectifs
# MAGIC 1. Vérifier la disponibilité du dataset Million Song Dataset
# MAGIC 2. Extraire les données vers votre volume en format JSON
# MAGIC 3. Créer différents échantillons pour analyse
# MAGIC 4. Valider l'extraction et préparer pour le pipeline DLT

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 ÉTAPE 1: VÉRIFICATION DE LA DISPONIBILITÉ DU DATASET

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérifier si le dataset est disponible
# MAGIC SELECT 
# MAGIC   count(*) as total_files,
# MAGIC   min(input_file_name()) as first_file,
# MAGIC   max(input_file_name()) as last_file
# MAGIC FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explorer la structure des données
# MAGIC DESCRIBE parquet.`/databricks-datasets/songs/data-001/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aperçu des premières lignes
# MAGIC SELECT * 
# MAGIC FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 ÉTAPE 2: VÉRIFICATION DU VOLUME DE DESTINATION

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérifier votre volume existant
# MAGIC DESCRIBE VOLUME dbacademy.database_ops.labuser_volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lister le contenu actuel du volume
# MAGIC LIST '/Volumes/dbacademy/database_ops/labuser_volume/';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 ÉTAPE 3: EXTRACTION COMPLÈTE VERS JSON

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 1: Extraire tout en un seul fichier JSON (pour petits datasets)
# MAGIC COPY (
# MAGIC   SELECT 
# MAGIC     artist_id,
# MAGIC     artist_lat,
# MAGIC     artist_long,
# MAGIC     artist_location,
# MAGIC     artist_name,
# MAGIC     duration,
# MAGIC     end_of_fade_in,
# MAGIC     key,
# MAGIC     key_confidence,
# MAGIC     loudness,
# MAGIC     release,
# MAGIC     song_hotnes,
# MAGIC     song_id,
# MAGIC     start_of_fade_out,
# MAGIC     tempo,
# MAGIC     time_signature,
# MAGIC     time_signature_confidence,
# MAGIC     title,
# MAGIC     year,
# MAGIC     -- Ajouter des métadonnées utiles
# MAGIC     current_timestamp() as extraction_timestamp,
# MAGIC     input_file_name() as source_parquet_file
# MAGIC   FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC   WHERE artist_name IS NOT NULL 
# MAGIC     AND title IS NOT NULL 
# MAGIC     AND song_id IS NOT NULL
# MAGIC )
# MAGIC TO '/Volumes/dbacademy/database_ops/labuser_volume/songs_complete.json'
# MAGIC OPTIONS (
# MAGIC   'format' = 'JSON',
# MAGIC   'mode' = 'OVERWRITE',
# MAGIC   'multiLine' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 ÉTAPE 4: EXTRACTION PAR LOTS (RECOMMANDÉ POUR GROS DATASETS)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 2: Diviser en plusieurs fichiers par année (plus gérable)
# MAGIC COPY (
# MAGIC   SELECT 
# MAGIC     artist_id,
# MAGIC     artist_lat,
# MAGIC     artist_long,
# MAGIC     artist_location,
# MAGIC     artist_name,
# MAGIC     duration,
# MAGIC     end_of_fade_in,
# MAGIC     key,
# MAGIC     key_confidence,
# MAGIC     loudness,
# MAGIC     release,
# MAGIC     song_hotnes,
# MAGIC     song_id,
# MAGIC     start_of_fade_out,
# MAGIC     tempo,
# MAGIC     time_signature,
# MAGIC     time_signature_confidence,
# MAGIC     title,
# MAGIC     year,
# MAGIC     current_timestamp() as extraction_timestamp
# MAGIC   FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC   WHERE year IS NOT NULL 
# MAGIC     AND year >= 2000  -- Données récentes seulement
# MAGIC     AND artist_name IS NOT NULL 
# MAGIC     AND title IS NOT NULL
# MAGIC )
# MAGIC TO '/Volumes/dbacademy/database_ops/labuser_volume/songs_2000s/'
# MAGIC OPTIONS (
# MAGIC   'format' = 'JSON',
# MAGIC   'mode' = 'OVERWRITE',
# MAGIC   'partitionBy' = 'year'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 ÉTAPE 5: EXTRACTION PAR ARTISTE (POUR ANALYSE SPÉCIFIQUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 3: Top artistes avec le plus de chansons
# MAGIC COPY (
# MAGIC   SELECT 
# MAGIC     artist_id,
# MAGIC     artist_lat,
# MAGIC     artist_long,
# MAGIC     artist_location,
# MAGIC     artist_name,
# MAGIC     duration,
# MAGIC     end_of_fade_in,
# MAGIC     key,
# MAGIC     key_confidence,
# MAGIC     loudness,
# MAGIC     release,
# MAGIC     song_hotnes,
# MAGIC     song_id,
# MAGIC     start_of_fade_out,
# MAGIC     tempo,
# MAGIC     time_signature,
# MAGIC     time_signature_confidence,
# MAGIC     title,
# MAGIC     year,
# MAGIC     row_number() OVER (PARTITION BY artist_name ORDER BY year) as song_number_by_artist
# MAGIC   FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC   WHERE artist_name IN (
# MAGIC     SELECT artist_name 
# MAGIC     FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC     WHERE artist_name IS NOT NULL
# MAGIC     GROUP BY artist_name 
# MAGIC     HAVING count(*) >= 10  -- Artistes avec au moins 10 chansons
# MAGIC     ORDER BY count(*) DESC 
# MAGIC     LIMIT 100  -- Top 100 artistes
# MAGIC   )
# MAGIC )
# MAGIC TO '/Volumes/dbacademy/database_ops/labuser_volume/top_artists/'
# MAGIC OPTIONS (
# MAGIC   'format' = 'JSON',
# MAGIC   'mode' = 'OVERWRITE',
# MAGIC   'partitionBy' = 'artist_name'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 ÉTAPE 6: EXTRACTION ÉCHANTILLON POUR TESTS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 4: Échantillon pour développement et tests
# MAGIC COPY (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as extraction_timestamp,
# MAGIC     'sample_dataset' as data_source
# MAGIC   FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC   WHERE song_id IS NOT NULL
# MAGIC   ORDER BY RAND()  -- Ordre aléatoire
# MAGIC   LIMIT 1000  -- 1000 chansons pour les tests
# MAGIC )
# MAGIC TO '/Volumes/dbacademy/database_ops/labuser_volume/sample_1000.json'
# MAGIC OPTIONS (
# MAGIC   'format' = 'JSON',
# MAGIC   'mode' = 'OVERWRITE'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ ÉTAPE 7: VÉRIFICATION DE L'EXTRACTION

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lister les fichiers créés dans votre volume
# MAGIC LIST '/Volumes/dbacademy/database_ops/labuser_volume/';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérifier le contenu des fichiers JSON créés
# MAGIC SELECT * 
# MAGIC FROM read_files('/Volumes/dbacademy/database_ops/labuser_volume/sample_1000.json', format => 'json')
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compter les enregistrements extraits
# MAGIC SELECT 
# MAGIC   'Original Parquet' as source,
# MAGIC   count(*) as record_count
# MAGIC FROM parquet.`/databricks-datasets/songs/data-001/`
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Extracted JSON' as source,
# MAGIC   count(*) as record_count
# MAGIC FROM read_files('/Volumes/dbacademy/database_ops/labuser_volume/sample_1000.json', format => 'json');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 ÉTAPE 8: STATISTIQUES DU DATASET EXTRAIT

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Statistiques générales
# MAGIC SELECT 
# MAGIC   count(*) as total_songs,
# MAGIC   count(DISTINCT artist_name) as unique_artists,
# MAGIC   count(DISTINCT year) as unique_years,
# MAGIC   min(year) as earliest_year,
# MAGIC   max(year) as latest_year,
# MAGIC   avg(duration) as avg_duration_seconds,
# MAGIC   avg(tempo) as avg_tempo
# MAGIC FROM read_files('/Volumes/dbacademy/database_ops/labuser_volume/sample_1000.json', format => 'json');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 artistes par nombre de chansons
# MAGIC SELECT 
# MAGIC   artist_name,
# MAGIC   count(*) as song_count,
# MAGIC   avg(song_hotnes) as avg_hotness,
# MAGIC   min(year) as first_year,
# MAGIC   max(year) as last_year
# MAGIC FROM read_files('/Volumes/dbacademy/database_ops/labuser_volume/sample_1000.json', format => 'json')
# MAGIC WHERE artist_name IS NOT NULL
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY song_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 COMMANDES UTILITAIRES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nettoyer le volume si nécessaire (décommentez si besoin)
# MAGIC -- REMOVE '/Volumes/dbacademy/database_ops/labuser_volume/songs_complete.json';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Créer un backup des données extraites (décommentez si besoin)
# MAGIC -- COPY (SELECT * FROM read_files('/Volumes/dbacademy/database_ops/labuser_volume/sample_1000.json', format => 'json'))
# MAGIC -- TO '/Volumes/dbacademy/database_ops/labuser_volume/backup/'
# MAGIC -- OPTIONS ('format' = 'DELTA', 'mode' = 'OVERWRITE');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 RÉSUMÉ ET ÉTAPES SUIVANTES
# MAGIC
# MAGIC ### ✅ Ce qui a été extrait:
# MAGIC - **`songs_complete.json`**: Dataset complet en un seul fichier
# MAGIC - **`songs_2000s/`**: Données des années 2000+ partitionnées par année
# MAGIC - **`top_artists/`**: Top 100 artistes avec le plus de chansons
# MAGIC - **`sample_1000.json`**: Échantillon de 1000 chansons pour tests
# MAGIC
# MAGIC ### 🔄 Prochaines étapes:
# MAGIC 1. **Vérifier** que l'extraction s'est bien déroulée
# MAGIC 2. **Configurer** votre pipeline Delta Live Tables
# MAGIC 3. **Utiliser** les fichiers JSON extraits comme source
# MAGIC 4. **Tester** avec l'échantillon avant le dataset complet
# MAGIC
# MAGIC ### 💡 Utilisation dans DLT:
# MAGIC ```sql
# MAGIC FROM STREAM(
# MAGIC   read_files(
# MAGIC     "/Volumes/dbacademy/database_ops/labuser_volume/*.json",
# MAGIC     format => "json",
# MAGIC     multiLine => true
# MAGIC   )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 📞 Support:
# MAGIC Si vous rencontrez des problèmes, vérifiez:
# MAGIC - Les permissions sur le volume
# MAGIC - La disponibilité du dataset source
# MAGIC - L'espace disponible dans le volume

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **📝 Notes importantes:**
# MAGIC - Les extractions peuvent prendre du temps selon la taille du dataset
# MAGIC - Utilisez l'échantillon (`sample_1000.json`) pour tester votre pipeline DLT
# MAGIC - Le dataset complet contient environ 1 million de chansons
# MAGIC - Les fichiers sont prêts à être utilisés dans Delta Live Tables
