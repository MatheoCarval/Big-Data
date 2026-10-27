#!/bin/sh
# Crée les buckets du Lakehouse (s3a://bronze, s3a://silver, s3a://gold). Relançable sans effet de bord.
set -eu

mc alias set lac http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in bronze silver gold; do
  mc mb --ignore-existing "lac/$bucket"
done

# Conservation. Bronze (copie brute, historisée) : versioning S3, pour récupérer un fichier supprimé ou écrasé par erreur ; les versions remplacées expirent après 30 jours pour que le bucket ne grossisse pas indéfiniment. 
# Silver et Gold restent sans versioning : Delta garde déjà leur historique (time travel) et VACUUM supprime les anciens fichiers ; avec le versioning, MinIO les conserverait et l'espace ne serait jamais libéré.
mc version enable lac/bronze
# mc ilm rule add n'a pas d'identifiant : on repart de zéro à chaque exécution, sinon les règles s'empilent
if mc ilm rule ls lac/bronze >/dev/null 2>&1; then
  mc ilm rule rm --all --force lac/bronze
fi
mc ilm rule add --noncurrent-expire-days 30 --expire-delete-marker lac/bronze

mc ls lac
