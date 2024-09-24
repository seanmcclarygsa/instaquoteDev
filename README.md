# r6InstaQuoteProcessor

This project is managed by Region 6 - used to support Real-time .
Work-in-Progress (WIP) Quotes for participating vendors for processing of 
quotes through the HACMAN RPA Bot

Original Project Created by Satish Venkatamaran 6/10/22 in GSS/BOD
Supported by Curtis Gathright in R6 - IT Program Manager for R6 HAC Directors 
office beginning 5/10/23

Program is run on a daily basis that ingests vendor quotes, uploads them to a 
SQL Server, executes a Stored Procedure that processes quote data, loads 
results into a SQL Table for transfer into a specific Google Sheet that the 
HACMAN process utilizes.

## Requirements

This program utilizes the [GSA CommonUtils](https://github.com/GSA/commonUtils) 
and [d2dPandasitel Libraries](https://github.com/satishvenkataraman/d2d-pandas-etl)
It sends emails via the SendEmails.py within CommonUtils which requires an 
authentication certificate to be included. See the link within that file for 
details.
