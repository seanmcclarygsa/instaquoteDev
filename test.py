import os
import sqlalchemy as sa
from QuoteProcessingOrchestrator import QuoteProcessingOrchestrator
import gspread
from oauth2client.service_account import ServiceAccountCredentials





#Instance
orchestrator = QuoteProcessingOrchestrator(processType="LoadAndProcessQuotes",configFromUI=None)

#Running setup for test
orchestrator.setup()


ld = os.listdir("Z:\\DATALOAD\\SeanM\\")
is_replace_type = 'Incremental'

engine = sa.create_engine("mssql+pyodbc://BODDBServer")

#connection = engine.connect()
#dbapi_conn = connection.connection
params = ",".join(ld)
print (params)
connection = engine.raw_connection()
try:
    cursor_obj = connection.cursor()
    cursor_obj.execute('sp_r6QuoteProcessor ?,?', params, is_replace_type)
 #   results = list(cursor_obj.fetchall())
    cursor_obj.close()
    connection.commit()
finally:
    connection.close()

'''
with engine.begin() as conn:  # transaction starts here
#				conn.execute(sa.text("EXEC [dbo].[sp_r6QuoteProcessor];"))
	params = ",".join(ld)
	print(params)
#	s=sa.text("EXEC [dbo].[sp_r6QuoteProcessor] :fileList :replaceType")
#	s=sa.text("EXEC [dbo].[sp_r6QuoteProcessor] :fileList :replaceType")
#	print(s)
#	conn.execute(s,fileList=params, replaceType=is_replace_type)
#	conn.execute(s,fileList=params,replaceType=is_replace_type)
	cursor = conn.cursor()
	cursor.callproc("sp_r6QuoteProcessor",[params],[is_replace_type])
	cursor.close()
	'''