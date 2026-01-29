# Projet PLE - Clash Royale

Ce projet vise à analyser les données de jeu de Clash Royale en utilisant Apache Hadoop pour traiter de grandes quantités de données.
Pour lancer les jobs, suivez les instructions ci-dessous.

``` bash
mvn compile
mvn package
scp target/clash_royale_ple-1.0.jar user@cluster:.
```

## Utilisation du Pipeline Hadoop

1. **Cleaning** : `yarn jar ... filter <in> <out> <nb_reducers>`

2. **Génération du Graphe** : `yarn jar ... graph <in> <out> <taille_archetype> <nb_reducers>`
   * Note : La taille doit être entre 1 et 8.
  
3. **Calcul des Stats** : `yarn jar ... stats <nodes_path> <edges_path> <out>`
   * <nodes_path> : dossier contenant les fichiers nodes-r-XXXXX
   * <edges_path> : dossier contenant les fichiers edges-r-XXXXX

## Analyser les résultats

Une fois les jobs terminés, vous devrez copier les résultats obtenus par le job Stats depuis le HDFS vers votre machine locale dans le dossier stats.
Les fichiers obtenus seront nommés `part-r-00000`, `part-r-00001`, etc. et devront être placés dans le dossier `stats/`.

### Pré-requis

``` bash
python -m venv .venv
source .venv/bin/activate
pip install numpy pandas matplotlib scipy
```

## Lancer le script d'analyse

``` bash
python3 result.py
```