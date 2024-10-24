@echo off

echo ========== Deploy Redpanda - Ref: Lecture 3 Exercise 3 ==========
cd /d %~dp0\deployment_files
kubectl apply -f redpanda.yaml
setlocal
:: Define the path for the temporary file
set "tempFile=%TEMP%\autoexec_temp.bat"
:: Write content to the temporary file
(
	echo @echo off
    echo echo Access Redpanda with: http://127.0.0.1:8080
	echo echo Waiting for pod Ready ^(timeout=20s^) . . .
    echo kubectl wait --for=condition=Ready pod -l app=redpanda --timeout=20s
	echo echo Start port-forward
	echo kubectl port-forward svc/redpanda 8080
) > "%tempFile%"
:: Execute the temporary file in a new terminal window
start "" "%tempFile%"
endlocal

pause