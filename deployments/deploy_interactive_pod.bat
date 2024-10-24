@echo off

echo ========== Deploy interactive pod - Ref: services\interactive - Alt. apache/hadoop:3 Ref: lecture 2 Exercise 2 ==========
echo Openning seperate cmd window with deployment
cd /d %~dp0\deployment_files
start cmd /k "echo Interactive pod! & kubectl run interactive -i --tty --image registry.gitlab.sdu.dk/jah/bigdatarepo/interactive:latest -- /bin/bash"
::start cmd /k "echo Interactive hadoop:3 pod! & kubectl run hadoop3 -i --tty --image apache/hadoop:3 -- /bin/bash"

pause