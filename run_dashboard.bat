@echo off
title Serverless Event Pipeline - Web Dashboard
cls
echo Starting Serverless Event-Driven Pipeline Live Control Room...
start http://127.0.0.1:5000
"%LOCALAPPDATA%\Python\bin\python.exe" app.py
pause
