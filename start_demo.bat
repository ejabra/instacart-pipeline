@echo off
TITLE Lancement de la Demo Instacart
echo ========================================================
echo 🚀 INITIALISATION DU PIPELINE INSTACART (DEMO MODE)
echo ========================================================


echo.
echo [1/5] Lancement du Consumer Kafka (Ingestion ClickHouse)...
start "1. Consumer Kafka" cmd /k "python consumer.py"

echo.
echo [2/5] Lancement du Sync MongoDB (Cache Daemon)...
start "2. Sync MongoDB" cmd /k "python mongodb/cache_to_mongodb.py --daemon"

echo.
echo [3/5] Lancement du Dashboard Business (Power BI / Stats)...
:: Port 8501 par défaut
start "3. Streamlit BI" cmd /k "streamlit run streamlit_app/app.py --server.port 8501"

echo.
echo [4/5] Entrainement du modele ML (Background)...
:: On lance l'entrainement mais on n'attend pas qu'il finisse pour lancer le reste
start "4. Training ML" cmd /k "python streamlit_app/ml_train.py --daemon"

echo.
echo ✅ TOUS LES SERVICES SONT LANCES !
echo.
echo Appuyez sur une touche pour fermer ce lanceur (les services resteront ouverts).
pause