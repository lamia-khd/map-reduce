@echo off
setlocal enabledelayedexpansion
title Hadoop Cluster Manager - TP2 Big Data

echo ======================================================================
echo           HADOOP CLUSTER CONTROLLER - TP 2 MAPREDUCE
echo ======================================================================
echo.

:: 1. Verifier si Docker fonctionne
echo [1/5] Verification de Docker...
docker version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERREUR] Docker n'est pas demarre. Lancez Docker Desktop d'abord.
    goto :end
)
echo [OK] Docker est actif.
echo.

:: 2. Demarrer les conteneurs du cluster
echo [2/5] Demarrage des conteneurs Hadoop (master, worker1, worker2)...
docker start hadoop-master hadoop-worker1 hadoop-worker2 >nul 2>&1
echo [OK] Conteneurs demarres.
echo.

:: 3. Lancer les daemons HDFS et YARN
echo [3/5] Demarrage des daemons HDFS et YARN (ResourceManager, NodeManagers)...
docker exec hadoop-master /opt/hadoop/bin/hdfs --daemon start namenode >nul 2>&1
docker exec hadoop-master /opt/hadoop/bin/hdfs --daemon start datanode >nul 2>&1
docker exec hadoop-worker1 /opt/hadoop/bin/hdfs --daemon start datanode >nul 2>&1
docker exec hadoop-worker2 /opt/hadoop/bin/hdfs --daemon start datanode >nul 2>&1

docker exec hadoop-master /opt/hadoop/bin/yarn --daemon start resourcemanager >nul 2>&1
docker exec hadoop-worker1 /opt/hadoop/bin/yarn --daemon start nodemanager >nul 2>&1
docker exec hadoop-worker2 /opt/hadoop/bin/yarn --daemon start nodemanager >nul 2>&1
echo [OK] Daemons Hadoop lances.
echo.

:: 4. Attente de stabilisation et sortie du safemode
echo [4/5] Verification de l'etat HDFS et Safemode...
timeout /t 5 /nobreak >nul
docker exec hadoop-master /opt/hadoop/bin/hdfs dfsadmin -safemode leave >nul 2>&1
echo [OK] HDFS Safemode OFF.
echo.

:: 5. Menu interactif
echo [5/5] Cluster Operationnel !
echo.
echo ======================================================================
echo   Interfaces Web disponibles sur votre machine :
echo     - YARN ResourceManager : http://localhost:8088
echo     - NameNode HDFS        : http://localhost:9870
echo ======================================================================
echo.

:menu
echo Options disponibles :
echo   [1] Ouvrir l'interface YARN (http://localhost:8088)
echo   [2] Verifier l'etat du cluster (dfsadmin report + yarn nodes)
echo   [3] Executer la Question 4 (Lancer PaymentJob sur purchases.txt)
echo   [4] Afficher le resultat final de la Question 4 (cat part-r-00000)
echo   [5] Ouvrir un bash interactif dans hadoop-master
echo   [6] Quitter
echo.
set /p choice="Entrez votre choix (1-6) : "

if "%choice%"=="1" (
    start http://localhost:8088
    echo YARN Web UI ouverte dans votre navigateur.
    echo.
    goto :menu
)
if "%choice%"=="2" (
    echo.
    echo --- Rapport HDFS ---
    docker exec hadoop-master /opt/hadoop/bin/hdfs dfsadmin -report
    echo.
    echo --- Noeuds YARN ---
    docker exec hadoop-master /opt/hadoop/bin/yarn node -list
    echo.
    goto :menu
)
if "%choice%"=="3" (
    echo.
    echo Execution de la Question 4 :
    echo   1. Suppression de l'ancien dossier de sortie...
    docker exec hadoop-master /opt/hadoop/bin/hdfs dfs -rm -r -f /user/root/purchases_output_q4
    echo   2. Copie du JAR vers le master...
    docker cp target\wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/tp2.jar
    docker exec -u 0 hadoop-master chmod 755 /root
    docker exec -u 0 hadoop-master chmod 644 /root/tp2.jar
    echo   3. Lancement du job MapReduce...
    echo      (Suivez en direct la progression sur http://localhost:8088)
    docker exec hadoop-master /opt/hadoop/bin/hadoop jar /root/tp2.jar hadoop.mapreduce.tp2.PaymentJob /user/root/input/purchases.txt /user/root/purchases_output_q4
    echo.
    echo [TERMINE] Job execute avec succes !
    echo.
    goto :menu
)
if "%choice%"=="4" (
    echo.
    echo --- Resultat dans HDFS (/user/root/purchases_output_q4/part-r-00000) ---
    docker exec hadoop-master /opt/hadoop/bin/hdfs dfs -cat /user/root/purchases_output_q4/part-r-00000
    echo.
    goto :menu
)
if "%choice%"=="5" (
    echo Ouverture du terminal bash dans hadoop-master...
    docker exec -it hadoop-master bash
    goto :menu
)
if "%choice%"=="6" (
    goto :end
)

echo Choix invalide.
goto :menu

:end
echo.
echo Au revoir !
pause

