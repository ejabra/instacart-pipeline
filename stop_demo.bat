@echo off
echo ARRET DE TOUS LES PROCESSUS PYTHON ET STREAMLIT...
taskkill /F /IM python.exe
taskkill /F /IM streamlit.exe
echo.
echo ✅ Tout est ferme.
pause