set mydir=C:\Users\seangmcclary\Documents\GitHub\r6InstaQuoteProcessor
set PYTHONPATH=%mydir%;%mydir%\commonUtils;%mydir%\d2d_pandas_etl

:: $env:mydir="C:\Users\seangmcclary\Documents\GitHub\r6InstaQuoteProcessor"

:: $env:PYTHONPATH="$env:mydir;$env:mydir\commonUtils;$env:mydir\pandasetlframework"

python QuoteProcessingOrchestrator.py LoadAndProcessQuotes