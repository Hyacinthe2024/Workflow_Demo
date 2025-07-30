# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration de l'Architecture Médaillon - Databricks Academy
# MAGIC
# MAGIC Ce notebook configure la structure complète du catalogue `dbacademy` avec l'architecture médaillon (Bronze, Silver, Gold)
# MAGIC
# MAGIC ## Structure du projet :
# MAGIC - **Catalogue** : `dbacademy`
# MAGIC - **Schémas** : `information_schema`, `ops`
# MAGIC - **Volume** : `labuser107148951750700...` (contient les données sources)
# MAGIC - **Tables** : `customers`, `orders`, `status` (Bronze, Silver, Gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration initiale et imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

# Configuration des variables globales
CATALOG_NAME = "dbacademy"
SCHEMA_NAME = "database_ops"
VOLUME_NAME = "labuser_volume"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

print(f"Configuration:")
print(f"- Catalogue: {CATALOG_NAME}")
print(f"- Schéma: {SCHEMA_NAME}")
print(f"- Volume: {VOLUME_NAME}")
print(f"- Chemin du volume: {VOLUME_PATH}")

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# import json
# from datetime import datetime

# # Configuration des variables globales
# CATALOG_NAME = "dbacademy"
# BRONZE_SCHEMA_NAME = "bronze_db"
# SILVER_SCHEMA_NAME = "silver_db"
# GOLD_SCHEMA_NAME = "gold_db"
# VOLUME_NAME = "labuser_volume"

# BRONZE_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA_NAME}/{VOLUME_NAME}"
# SILVER_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SILVER_SCHEMA_NAME}/{VOLUME_NAME}"
# GOLD_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{GOLD_SCHEMA_NAME}/{VOLUME_NAME}"

# print(f"Configuration:")
# print(f"- Bronze Volume Path: {BRONZE_VOLUME_PATH}")
# print(f"- Silver Volume Path: {SILVER_VOLUME_PATH}")
# print(f"- Gold Volume Path: {GOLD_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Création de la structure du catalogue

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création du catalogue principal
# MAGIC CREATE CATALOG IF NOT EXISTS dbacademy;
# MAGIC
# MAGIC -- Utilisation du catalogue
# MAGIC USE CATALOG dbacademy;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création du schéma information_schema (si pas déjà existant)
# MAGIC CREATE SCHEMA IF NOT EXISTS dbacademy.information_schema;
# MAGIC
# MAGIC -- Création du schéma ops pour les tables médaillon
# MAGIC CREATE SCHEMA IF NOT EXISTS dbacademy.database_ops;
# MAGIC DROP  SCHEMA IF EXISTS dbacademy.ops;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création du volume pour les données sources
# MAGIC CREATE VOLUME IF NOT EXISTS dbacademy.database_ops.labuser_volume;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Génération des données d'exemple en format JSON

# COMMAND ----------

# Génération des données d'exemple pour customers
customers_data = [
    {"customer_id": 1, "first_name": "Jean", "last_name": "Dupont", "email": "jean.dupont@email.com", "phone": "0123456789", "address": "123 Rue de la Paix, Paris", "created_at": "2023-01-15T10:30:00Z"},
    {"customer_id": 2, "first_name": "Marie", "last_name": "Martin", "email": "marie.martin@email.com", "phone": "0987654321", "address": "456 Avenue des Champs, Lyon", "created_at": "2023-02-20T14:15:00Z"},
    {"customer_id": 3, "first_name": "Pierre", "last_name": "Bernard", "email": "pierre.bernard@email.com", "phone": "0147258369", "address": "789 Boulevard Saint-Germain, Marseille", "created_at": "2023-03-10T09:45:00Z"},
    {"customer_id": 4, "first_name": "Sophie", "last_name": "Rousseau", "email": "sophie.rousseau@email.com", "phone": "0162534897", "address": "321 Rue Victor Hugo, Toulouse", "created_at": "2023-04-05T16:20:00Z"},
    {"customer_id": 5, "first_name": "Lucas", "last_name": "Moreau", "email": "lucas.moreau@email.com", "phone": "0173951426", "address": "654 Place de la République, Nice", "created_at": "2023-05-12T11:10:00Z"}
]

# Génération des données d'exemple pour orders
orders_data = [
    {"order_id": 1001, "customer_id": 1, "order_date": "2023-06-15T08:30:00Z", "total_amount": 129.99, "currency": "EUR", "status_id": 1},
    {"order_id": 1002, "customer_id": 2, "order_date": "2023-06-16T12:45:00Z", "total_amount": 89.50, "currency": "EUR", "status_id": 2},
    {"order_id": 1003, "customer_id": 1, "order_date": "2023-06-17T15:20:00Z", "total_amount": 234.75, "currency": "EUR", "status_id": 1},
    {"order_id": 1004, "customer_id": 3, "order_date": "2023-06-18T10:15:00Z", "total_amount": 156.30, "currency": "EUR", "status_id": 3},
    {"order_id": 1005, "customer_id": 4, "order_date": "2023-06-19T14:00:00Z", "total_amount": 67.80, "currency": "EUR", "status_id": 1},
    {"order_id": 1006, "customer_id": 5, "order_date": "2023-06-20T09:30:00Z", "total_amount": 198.45, "currency": "EUR", "status_id": 2},
    {"order_id": 1007, "customer_id": 2, "order_date": "2023-06-21T16:45:00Z", "total_amount": 312.20, "currency": "EUR", "status_id": 4}
]

# Génération des données d'exemple pour status
status_data = [
    {"status_id": 1, "status_name": "Completed", "status_description": "Order completed successfully"},
    {"status_id": 2, "status_name": "Processing", "status_description": "Order is being processed"},
    {"status_id": 3, "status_name": "Shipped", "status_description": "Order has been shipped"},
    {"status_id": 4, "status_name": "Cancelled", "status_description": "Order has been cancelled"}
]

print("Données d'exemple générées:")
print(f"- {len(customers_data)} clients")
print(f"- {len(orders_data)} commandes")
print(f"- {len(status_data)} statuts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sauvegarde des fichiers JSON dans le volume

# COMMAND ----------

# DBTITLE 1,SIMULATION
import json

# Sauvegarde des données JSON dans le volume
def save_json_to_volume(data, filename):
    json_path = f"{VOLUME_PATH}/{filename}"
    
    # Conversion en JSON string
    json_string = "\n".join([json.dumps(record) for record in data])
    
    # Écriture dans le volume (simulation - en réalité, vous utiliseriez dbutils.fs.put)
    print(f"Sauvegarde de {len(data)} enregistrements dans {json_path}")
    return json_string

# Sauvegarde des fichiers
customers_json = save_json_to_volume(customers_data, "customers.json")
orders_json = save_json_to_volume(orders_data, "orders.json")
status_json = save_json_to_volume(status_data, "status.json")

# COMMAND ----------

# DBTITLE 1,ICI ON CHAGE REELLEMENT
# Sauvegarde des données JSON dans le volume avec dbutils
def save_json_to_volume(data, filename):
    json_path = f"{VOLUME_PATH}/{filename}"
    json_string = "\n".join([json.dumps(record) for record in data])
    dbutils.fs.put(json_path, json_string, overwrite=True)
    print(f"Sauvegarde de {len(data)} enregistrements dans {json_path}")
    return json_string

customers_json = save_json_to_volume(customers_data, "customers.json")
orders_json = save_json_to_volume(orders_data, "orders.json")
status_json = save_json_to_volume(status_data, "status.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GENERALITE
# MAGIC
# MAGIC Le code de cette cellule effectue les opérations suivantes :
# MAGIC
# MAGIC - **Importation du module `requests`** : utilisé pour effectuer des requêtes HTTP en Python.
# MAGIC
# MAGIC - **Définition de la fonction `load_json_from_source`** :
# MAGIC   - Prend un argument `source` (URL ou chemin de fichier local).
# MAGIC   - Si `source` commence par "http://" ou "https://", effectue une requête GET pour récupérer les données JSON depuis l'URL.
# MAGIC   - Sinon, ouvre le fichier local et charge les données JSON.
# MAGIC
# MAGIC - **Définition de la fonction `save_json_to_volume_from_source`** :
# MAGIC   - Prend deux arguments : `source` (source des données JSON) et `filename` (nom du fichier de sauvegarde).
# MAGIC   - Charge les données JSON via `load_json_from_source`.
# MAGIC   - Construit le chemin complet du fichier avec la variable globale `VOLUME_PATH` et le nom de fichier.
# MAGIC   - Convertit les données en chaîne JSON avec une compréhension de liste et `json.dumps`.
# MAGIC   - Écrit les données JSON dans le volume avec `dbutils.fs.put` (option `overwrite=True`).
# MAGIC   - Affiche un message indiquant le nombre d'enregistrements sauvegardés et le chemin du fichier.
# MAGIC
# MAGIC - **Exemple d'utilisation** :
# MAGIC   - Les données des clients, commandes et statuts sont sauvegardées en appelant `save_json_to_volume_from_source` avec les chemins de fichiers locaux et les noms de fichiers appropriés.

# COMMAND ----------

# DBTITLE 1,GENERALITE
# Lecture des données JSON à partir d'une source (ex: fichier local ou URL) et sauvegarde dans le volume
# import requests  # Importation du module requests pour les requêtes HTTP

# Fonction pour charger des données JSON depuis une source (URL ou fichier local)
# def load_json_from_source(source):
#     # Si la source est une URL HTTP/HTTPS
#     if source.startswith("http://") or source.startswith("https://"):
#         response = requests.get(source)  # Récupère le contenu via HTTP
#         response.raise_for_status()      # Lève une exception si la requête a échoué
#         data = response.json()           # Charge le JSON depuis la réponse
#     else:
#         # Sinon, lit le fichier localement
#         with open(source, "r", encoding="utf-8") as f:
#             data = json.load(f)          # Charge le JSON depuis le fichier
#     return data

# Fonction pour sauvegarder les données JSON chargées depuis une source dans le volume Databricks
# def save_json_to_volume_from_source(source, filename):
#     data = load_json_from_source(source)  # Charge les données depuis la source
#     json_path = f"{VOLUME_PATH}/{filename}"  # Définit le chemin de sauvegarde dans le volume
#     json_string = "\n".join([json.dumps(record) for record in data])  # Convertit les données en lignes JSON
#     dbutils.fs.put(json_path, json_string, overwrite=True)  # Sauvegarde dans le volume Databricks
#     print(f"Sauvegarde de {len(data)} enregistrements dans {json_path}")  # Affiche un message de confirmation
#     return json_string

# Exemple d'utilisation avec des chemins de fichiers ou URLs
#customers_json = save_json_to_volume_from_source("/dbfs/tmp/customers.json", "customers.json")
#orders_json = save_json_to_volume_from_source("/dbfs/tmp/orders.json", "orders.json")
#status_json = save_json_to_volume_from_source("/dbfs/tmp/status.json", "status.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Création des tables Bronze (données brutes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Création des tables Silver (données nettoyées et transformées)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Création des tables Gold (données agrégées et optimisées pour l'analyse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Vérification de la structure complète

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Affichage de la structure du catalogue
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Affichage des schémas dans dbacademy
# MAGIC SHOW SCHEMAS IN dbacademy;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Affichage des tables dans le schéma ops
# MAGIC SHOW TABLES IN dbacademy.database_ops;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Affichage des volumes
# MAGIC SHOW VOLUMES IN dbacademy.database_ops;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Tests de validation des données

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Exemples de requêtes d'analyse
