set mydir=C:\users\CurtisMGathright\Python
set PYTHONPATH=%mydir%;%mydir%\pythoncode\commonUtils;%mydir%\pythoncode\d2d_pandas_etl;C:\Users\CurtisMGathright\AppData\Roaming\Python\Python39\site-packages;C:\Users\CurtisMGathright\AppData\Roaming\Python\Python39\Scripts;C:\Program Files\Anaconda3\pkgs\sqlite-3.38.2-h2bbff1b_0\Library\bin

FOR /F %%A IN ('WMIC OS GET LocalDateTime ^| FINDSTR \.') DO @SET B=%%A
rem echo %B%
for /f "delims=." %%i in ("%B%") do @SET DT=%%i
rem echo %DT%

python QuoteProcessingOrchestrator.py LoadAndProcessQuotes>logs/log_%DT%.txt
