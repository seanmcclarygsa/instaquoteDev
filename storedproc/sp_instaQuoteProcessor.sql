USE [QS_QUERY]
GO
/****** Object:  StoredProcedure [dbo].[sp_instaQuoteProcessor]    Script Date: 3/7/2024 1:43:38 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Satish Venkataraman>
-- Create date: <June 10, 2022>
-- Description:	<This is the stored procedure done to support the automation for R6 Quote Processing.
--              This will be called by the Python Program>
-- 5/26/2023 - CMG - Updated to send status emails and log to instaquote_Status_Log
-- 6/5/2023 - CMG - updated Z: Folder file landing and resources to z:\Dataload\R6_Instaquote\
-- 6/15/2023 - CMG - updated to add SQL logging steps in error handling
-- 7/26/2023 - CMG - added exclusion to EUCOMM/NDOCOMM to exclude these contracts (beginning with X) per Stephen K.
-- 10/3/2023 - CMG - updates to what is include exclusion reason and match description to Staging Table
-- 11/28/2023 - CMG - Major updates to acount for change to NSN ingest process.
-- 12/19/2023 - CMG - Updates to account for FCS Orders - pull in cage code from a google sheet via Pythonto populate CAGE CODE for these orders
-- 02/27/2024 - CMG - Updates to add in RPTOFC N to #standingOrders and staging (NSNs only being incorporated at this point) 
--					  Hard coding for Office M removed.				
-- =============================================
ALTER PROCEDURE [dbo].[sp_instaQuoteProcessor] @fileList varchar(max), @replaceType varchar(max), @resultString varchar(max) output
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    DECLARE @EmailBody      varchar(max)
	DECLARE @strStatusStart varchar(max)
	DECLARE @errorString    varchar(max)
	--Next Line For Testing Only
	--declare @nsnfiles varchar(max), @pnfiles varchar(max),@fileList varchar(max),@resultString varchar(max), @replaceType varchar(max)
	select @strStatusStart = 'Parameters: replaceType:' + @replaceType + 'FileList: ' + @fileList
	--Set Log entry for start
	INSERT INTO Instaquote_Status_Log 
	(IQ_Status_Date, IQ_Status_StoredProc, IQ_Status_Python, IQ_Status_Comments)
	Values(getDate(), 'STORED PROCEDURE STARTED', null, @strStatusStart)


	SET @resultString=''
	--Loading InstaQuote Catalogs
	IF UPPER(@replaceType) = 'PARTIAL'
	BEGIN
    --  PRINT('ADDING TO INGEST TABLE ... ')
		SET @resultString = @resultString + '||ADDING TO INGEST TABLE ... '
	END
	ELSE
	BEGIN
  --      PRINT('CLEANING UP EXISTING INGEST TABLE BEFORE STARTING ... ')
		SET @resultString = @resultString + '||CLEANING UP EXISTING INGEST TABLE ... '
		TRUNCATE table qs_query.dbo.instaQuotes_ingest
	END


-- FOR TESTING PURPOSES
--declare @nsnfiles varchar(max), @pnfiles varchar(max),@fileList varchar(max),@resultString varchar(max), @replaceType varchar(max)
/*    SET @replaceType='complete'
set @nsnfiles='Central Power Systems & Services_NSN.tsv,Complete Packaging & Shipping Supplies_NSN.tsv,DMC_NSN.tsv,Giga_NSN.tsv,Inserts & Kits_NSN.tsv,Kaufman_NSN.tsv,Kipper_NSN.tsv,Mid-Continent Machining_NSN.tsv,Premier_NSN.tsv,Timber Edge Machine_NSN.tsv,Urenda_NSN.tsv,WTC_NSN.tsv'
set @pnfiles='Chester Supply_PN.tsv,Divine Imaging_PN.tsv,Emcor Crenlo_PN.tsv,KRCampbell Hot and Cold Plumbing_PN.tsv,Laine Industries_PN.tsv,MSC_PN.tsv,Noble Supply_PN.tsv,Partsmaster_PN.tsv,San Antonio Lighthouse_PN.tsv,Supply Chimp_PN.tsv,W.W.Grainger_PN.tsv'
set @fileList=@nsnfiles+','+@pnfiles
 SET @fileList = 'Mid-Continent Machining_NSN.tsv,Premier_NSN.tsv,Timber Edge Machine_NSN.tsv,Urenda_NSN.tsv,WTC_NSN.tsv'*/


-- SET @fileList = 'A&M INDUSTRIAL_NSN.tsv,ADA Supplies_NSN.tsv,ADA Supplies_PN.tsv,Aero Tools_NSN.tsv,Alpha Vets LLC_NSN.tsv,Alpha Vets LLC_PN.tsv,American Kal Enterprises Inc_NSN.tsv,ASC Industries_NSN.tsv,ASC Industries_PN.tsv,Astro Tools_PN.tsv,Astro Tool_NSN.tsv,Central Power Systems & Services_NSN.tsv,Central Power Systems & Services_PN.tsv,Chester Supply_PN.tsv,CPS Solutions_NSN.tsv,CPS Solutions_PN.tsv,Creative Logistics_NSN.tsv,Daniels Manufacturing Corp_NSN.tsv,Daniels Manufacturing Corp_PN.tsv,Divine Imaging_PN.tsv,Fiskars_NSN.tsv,Fiskars_PN.tsv,Forrest Tool_NSN.tsv,Forrest Tool_PN.tsv,Giga_NSN.tsv,Giga_PN.tsv,GMS Industrial Supply_NSN.tsv,GMS Industrial Supply_PN.tsv,Inserts & Kits_NSN.tsv,Kaufman_NSN.tsv,Kaufman_PN.tsv,Kipper _PN.tsv,Kipper_NSN.tsv,KRCampbell Hot and Cold Plumbing_PN.tsv,Laine Industries_PN.tsv,Lawson_NSN.tsv,Lawson_PN.tsv,LC Industries_NSN.tsv,LC Industries_PN.tsv,Mid-Continent Machining_NSN.tsv,Midwest Motor Supply Co Inc dba Kimball Midwest_NSN.tsv,Midwest Motor Supply Co Inc dba Kimball Midwest_PN.tsv,MSC Industrial Direct_NSN.tsv,MSC_PN.tsv,Noble Supply_PN.tsv,Pacific Ink_PN.tsv,Partsmaster_PN.tsv,Premier & Companies Inc_NSN.tsv,Premier and Company_PN.tsv,Safety Seal_NSN.tsv,San Antonio Lighthouse_NSN.tsv,San Antonio Lighthouse_PN.tsv,Snap On_NSN.tsv,Snap On_PN.tsv,Supplies Now_PN.tsv,Supply Chimp_NSN.tsv,Supply Chimp_PN.tsv,Timber Edge Machine_NSN.tsv,W.W.Grainger_PN.tsv,Wecsys_PN.tsv,Womack_NSN.tsv,Womack_PN.tsv,Wrigglesworth Enterprises_NSN.tsv,Wrigglesworth Enterprises_PN.tsv,WTC_NSN.tsv'
--11/8/2023 Troubleshooting

--set @fileList = 'A&M INDUSTRIAL_NSN.tsv, ADA Supplies_NSN.tsv, ADA Supplies_PN.tsv, Aero Tools_NSN.tsv, Alpha Vets LLC_NSN.tsv, Alpha Vets LLC_PN.tsv, American Kal Enterprises Inc_NSN.tsv, ASC Industries_NSN.tsv, ASC Industries_PN.tsv, Astro Tools_PN.tsv, Astro Tool_NSN.tsv, Central Power Systems & Services_NSN.tsv, Central Power Systems & Services_PN.tsv, Chester Supply_PN.tsv, CPS Solutions_NSN.tsv, CPS Solutions_PN.tsv, Creative Logistics_NSN.tsv, Daniels Manufacturing Corp_NSN.tsv, Daniels Manufacturing Corp_PN.tsv, Divine Imaging_PN.tsv, Fiskars_NSN.tsv, Fiskars_PN.tsv, Forrest Tool_NSN.tsv, Forrest Tool_PN.tsv, Giga_NSN.tsv, Giga_PN.tsv, GMS Industrial Supply_NSN.tsv, GMS Industrial Supply_PN.tsv, Greenfield Industries_NSN.tsv, Greenfield Industries_PN.tsv, Inserts & Kits_NSN.tsv, Kaufman_NSN.tsv, Kaufman_PN.tsv, Kipper _PN.tsv, Kipper_NSN.tsv, KRCampbell Hot and Cold Plumbing_PN.tsv, Laine Industries_PN.tsv, Lawson_NSN.tsv, Lawson_PN.tsv, LC Industries_NSN.tsv, LC Industries_PN.tsv, Mid-Continent Machining_NSN.tsv, Midwest Motor Supply Co Inc dba Kimball Midwest_NSN.tsv, Midwest Motor Supply Co Inc dba Kimball Midwest_PN.tsv, Military Operational Systems LLC_NSN.tsv, Military Operational Systems LLC_PN.tsv, MSC Industrial Direct_NSN.tsv, MSC_PN.tsv, Noble Supply_PN.tsv, Pacific Ink_PN.tsv, Partsmaster_PN.tsv, Premier & Companies Inc_NSN.tsv, Premier and Company_PN.tsv, Safety Seal_NSN.tsv, San Antonio Lighthouse_NSN.tsv, San Antonio Lighthouse_PN.tsv, Snap On_NSN.tsv, Snap On_PN.tsv, Supplies Now_PN.tsv, Supply Chimp_NSN.tsv, Supply Chimp_PN.tsv, Timber Edge Machine_NSN.tsv, W W Grainger_PN.tsv, Wecsys_PN.tsv, Womack_NSN.tsv, Womack_PN.tsv, Wrigglesworth Enterprises_NSN.tsv, Wrigglesworth Enterprises_PN.tsv, Wright Tool Company_NSN.tsv'		
    DROP TABLE IF EXISTS #tmp
	SELECT value into #tmp FROM STRING_SPLIT(@fileList, ',');

 ----   DECLARE @resultString varchar(max)
 ----   set @resultString=''
	DECLARE @fileName VARCHAR(max)
	DECLARE @compFN VARCHAR(max)
	DECLARE @sql VARCHAR(max)
	DECLARE @formatfile VARCHAR(max)
	DECLARE @view VARCHAR(20) -- In order to dynamically do a bulk insert, a view has been created which is set based on the file name
	DECLARE @pathFormats varchar(150)
	DECLARE @pathFiles varchar(150)
	SET @pathFormats = '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\FormatFiles\'
	SET @pathFiles = '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\Quotes\'

--    DECLARE @doesFileExist INT
	SET @resultString = @resultString + '||LOADING QUOTE FILES TO INGEST TABLE ...'
	WHILE (SELECT Count(*) FROM #tmp WHERE value is not null) > 0
	BEGIN
	   SELECT TOP 1 @fileName = value From #tmp
	   --If this is partial, first delete any existing entries for the given file
	   IF UPPER(@replaceType) = 'PARTIAL'
		BEGIN
		  DELETE FROM instaquotes_ingest where filename=@fileName
		  SET @resultString = @resultString + '||        Cleaning up existing ingest table for '+@fileName+' ... Done'
		END

	   --Check if #fileName contains NSN, if so set the BCPFormat appropriately
	   SET @compFN=@pathFiles+@fileName
	   set @compFN = ''''+@compFN+''''
--       PRINT('Loading file ... '+@fileName)
	   SET @resultString = @resultString + '||        Loading '+@fileName+' ... '

		IF @compFN like '%NSN%'
		   BEGIN
			   SET @formatfile = @pathFormats+'r6NSNFormat.xml'
			   SET @view = 'v_instaQuoteNSN';
		   END
		ELSE
		   BEGIN
				IF @compFN like '%PN%' 
					SET @formatfile =@pathFormats+'r6PNFormat.xml'
					SET @view = 'v_instaQuotePN'                
			END
		set @formatfile = ''''+@formatfile+''''
 --       PRINT(@formatfile)
--        PRINT(@VIEW)
		
		SET @sql='BULK INSERT '+@view+' FROM '+@compFN+' WITH 
		( 
			FIELDTERMINATOR = ''\t'',
			ROWTERMINATOR = ''0x0a'',
			FIRSTROW = 2,
			BATCHSIZE = 500000,
			FORMATFILE = '+@formatfile+'
		)'
--       PRINT(@sql)
	   BEGIN TRY
			EXEC (@sql)
			update instaQuotes_ingest set filename=@fileName where filename is null or filename=''
			SET @resultString = @resultString + 'Done'
	   END TRY
	   BEGIN CATCH
	      -- Save the error information into the @errorString variable instead of directly appending to @resultString
			SET @errorString = CONCAT('ErrorNumber: ', ERROR_NUMBER(), ' || ',
                              'ErrorState: ', ERROR_STATE(), ' || ',
                              'ErrorSeverity: ', ERROR_SEVERITY(), ' || ',
                              'ErrorProcedure: ', ERROR_PROCEDURE(), ' || ',
                              'ErrorLine: ', ERROR_LINE(), ' || ',
                              'ErrorMessage: ', ERROR_MESSAGE());
		-- Append Error to ResultString:
		 -- Append the error information to @resultString
			SET @resultString = @resultString + '||' + @errorString;
	   --Send EMAIL
	   EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor failed',
			@body = 'Stored Procedure Failed - Review Logs and Instaquote_Status_Log for more information',
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'
		
			INSERT INTO Instaquote_Status_Log 
			(IQ_Status_Date, IQ_Status_StoredProc, IQ_Status_Python, IQ_Status_Comments)
			Values(getDate(), 'Error at Ingest Update', null, @resultString )

			--Commented out 5/8/2023 due to error creation
			--5/26/23 -  CMG removed errant Colon within the below line and added email/logging
			--SET @resultString = @resultString + '||' + @fileName+' '+ERROR_MESSAGE()+' '+ERROR_LINE()
			SET @emailBody = Concat('Stored Procedure Failed - Review Logs and Instaquote_Status_Log for more information. Details ' , @resultString)
			EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor failed',
			@body = @emailBody,
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'

		END CATCH;
		
	   DELETE FROM #tmp WHERE value = @FileName
	END


--TODO: Enrich Ingest table to show description - below is code to obtain ItemDesc from SOLines (same as how NSNDemand retrieves - per Stephen)
;WITH cte AS (
    SELECT
        B.ItemID,
        B.ItemDesc,
        ROW_NUMBER() OVER (PARTITION BY B.ItemID ORDER BY B.ORDERLINEKEY DESC) AS RowNum
    FROM
        --fl_db.dbo.OMS_SOLines B
		FL_DB.dbo.OMS_SALESORDERS b 
)
UPDATE instaquotes_ingest
SET
    ItemDesc = left(REPLACE(REPLACE(qs_query.dbo.removeSpecialCharacters(B.ItemDesc), '''', ''), '"', ''), 200)

FROM
    instaquotes_ingest A
inner JOIN
    cte B ON A.NSN = B.ItemID AND B.RowNum = 1
WHERE
    A.ItemDesc IS NULL and a.itemType = 'NSN'

	
	
-- Delete those rows which are complete duplicates i.e. every column is exactly the same.
--    select top 10 * from instaQuotes_ingest
--    sp_columns instaquotes_ingest
    ;with cte(MFRNAME,MFRPN,UOI,PKGQTY,UNITPRICE,VENDORNAME,CONTRNUMBER,DARO,MINORDTHRESH,VALIDUNTIL,FOB,ITEMDESC,MAS_OM,COO,
NSN,MFRNAMEQUOTED,MFRPNQUOTED,ITEMTYPE,MFRCAGECODE,BNOE,ISBRANDNAMEPN,DOCUMENTATION,ACCEPTABLESUBSTITUTE,QUOTEDATE,SAMMFRNAME,filename,VENDORCAGECODE, DuplicateCount) as (select MFRNAME,MFRPN,UOI,PKGQTY,UNITPRICE,VENDORNAME,CONTRNUMBER,DARO,MINORDTHRESH,VALIDUNTIL,FOB,ITEMDESC,MAS_OM,COO,
NSN,MFRNAMEQUOTED,MFRPNQUOTED,ITEMTYPE,MFRCAGECODE,BNOE,ISBRANDNAMEPN,DOCUMENTATION,ACCEPTABLESUBSTITUTE,QUOTEDATE,SAMMFRNAME,filename,VENDORCAGECODE, ROW_NUMBER() over (PARTITION by MFRNAME,MFRPN,UOI,PKGQTY,UNITPRICE,VENDORNAME,CONTRNUMBER,DARO,MINORDTHRESH,VALIDUNTIL,FOB,ITEMDESC,MAS_OM,COO,
NSN,MFRNAMEQUOTED,MFRPNQUOTED,ITEMTYPE,MFRCAGECODE,BNOE,ISBRANDNAMEPN,DOCUMENTATION,ACCEPTABLESUBSTITUTE,QUOTEDATE,SAMMFRNAME,filename,VENDORCAGECODE order by VENDORNAME,MFRPN, QUOTEDATE desc ) as DuplicateCount from instaquotes_ingest)
delete from cte where DuplicateCount > 1

	-- Load the Standing Orders into #standingOrders
	--2/27/24 - Updated to test with Office N
	SET @resultString = @resultString + '||LOADING STANDING ORDERS ... '
	drop table if exists #standingOrders
	select NSN, REQNNO, w.ITEMDESC, CAGECODE, IsTrueNSN, UOM, w.RPTOFF,w.totalCost, w.nifestRevenue into #standingOrders
	from QS_DASHBOARD.dbo.WIP_DASH w left outer  join FL_DB.dbo.OMS_SOLINES l on w.REQNNO=l.REQUISITIONNO
	where (w.RPTOFF='M' or w.RptOFf = 'N') and w.Status='To Be Processed' and len(NSN)<>4 and istruensn='NO' 

	INSERT INTO #standingOrders
	select NSN, REQNNO, w.ITEMDESC, CAGECODE, IsTrueNSN, UOM, w.RPTOFF, w.totalcost, w.nifestRevenue
	from QS_DASHBOARD.dbo.WIP_DASH w left outer  join FL_DB.dbo.OMS_SOLINES l on w.REQNNO=l.REQUISITIONNO
	where (w.RPTOFF='M' or w.RptOff = 'N')and w.Status='To Be Processed' and  istruensn='YES' 

	SET @resultString = @resultString + 'Done'

	 -- Make certain updates to the intermediate staging table
	-- set the MFRNAMEQUOTED to be equal to MFRNAME for PN
	-- set the MFRPNQUOTED to be equal to the MFRPN for PN
	SET @resultString = @resultString + '||BUILDING THE INTERMEDIATE STAGING TABLE ...'
	drop table if exists #instaQuotes_im_staging
	select * into #instaQuotes_im_staging from instaQuotes_ingest 

	alter table #instaQuotes_im_staging
	ADD MOP  varchar(5),
	RPTOFF varchar(1),
	FARPART varchar(10),
	HORC varchar(1),
	INSTAQUOTE varchar(1),
	LPP decimal(9,2),
	CO varchar(30),
	BUYERCODE varchar(5),
	COWARRANTLEVEL varchar(20),
	BUYERNAME varchar(50),
	QUOTEDVSLPP decimal(9,2),
--	QUOTESOURCE varchar(20),
	EXCLUSIONREASON varchar(50),
    MATCHDESC varchar(50)

	update #instaQuotes_im_staging 
	set MFRNAMEQUOTED = MFRNAME,
		MFRPNQUOTED   = MFRPN
	where ITEMTYPE='PN'

	-- Match the instaquotes against the standing orders to see what really matches.
	SET @resultString = @resultString + '||    MATCHING STANDING ORDERS AGAINST INGESTED QUOTES ... '
	-- Matching for NSNs:

--    select count(distinct stg.NSN)

	update stg
	set EXCLUSIONREASON='EXPIRED QUOTE'
	from #instaQuotes_im_staging stg  
	where try_cast(stg.validuntil as date) < getDate()

	
	update stg
	set MATCHDESC='EXACT MATCH', rptOff = s.rptOff
	from #instaQuotes_im_staging stg left outer join #standingOrders s on s.NSN=stg.NSN and stg.UOI=s.UOM 
	where stg.ITEMTYPE='NSN'and s.IsTrueNSN='YES' and stg.EXCLUSIONREASON is null

--AB1 Table Match
	update stg
	set stg.EXCLUSIONREASON = 'AB1 Table Match'
	from #instaQuotes_im_staging stg 
	inner join [FL_DB].[dbo].[AB1_ETS] ab1 on ab1.sku = stg.mfrpn and stg.mfrname = ab1.mfgname
	where stg.ExclusionReason is null

	update stg 
	set EXCLUSIONREASON='Excluded due to BNOE' 
	from #instaQuotes_im_staging stg
	where ITEMTYPE='NSN' AND 
    (
    (BNOE='Yes' and ISBRANDNAMEPN='') OR 
    (BNOE='Yes' and ISBRANDNAMEPN='No' and (DOCUMENTATION='' or DOCUMENTATION='No')) OR
    (BNOE='Yes' and ISBRANDNAMEPN='No' and DOCUMENTATION='Yes' and (ACCEPTABLESUBSTITUTE='' or ACCEPTABLESUBSTITUTE='No'))
    )

	SET @resultString = @resultString + '||        for NSNs ...Done'

--    select * from #instaQuotes_staging where ITEMTYPE='NSN'
--    select * from #standingOrders where IsTrueNSN='YES' and nsn in (select nsn from #instaQuotes_staging where ITEMTYPE='NSN')

	-- Matching for PNs:
	-- Manufacturer Part Number - Must match exactly
	-- CAGE Code (If vendor provides CAGE Code in quotes, we can match it up against) - Optional 
	-- Item Description (fuzzy search)
	 --  select * from #instaQuotes_staging where ITEMTYPE='PN' and MATCHDESC is not null
 --   select MATCHDESC, count(*) from #instaQuotes_staging group by MATCHDESC

  -- Check if the item  that is on the WIP is a part number item that has got a corresponding NSN
    TRUNCATE TABLE instaquote_FLISDATA
    BULK INSERT instaquote_FLISDATA
       FROM '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\FormatFiles\NSN PN reference info FEDLOG.csv'
       WITH 
       ( -- DATAFILETYPE = 'char',
         FIELDTERMINATOR = ',',
         ROWTERMINATOR = '0x0a',
         FIRSTROW = 2,
         BATCHSIZE = 500000
       )

     
     create table #CombinedMapping_RC2PNNSN (
     NSN	varchar(13),
     NSNContactor varchar(75),
     Manufacturer	varchar(50),
     ManufacturerPN	varchar(75),
     Contract_Load varchar(25))

     BULK INSERT #CombinedMapping_RC2PNNSN
       FROM '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\Mappings\CombinedMapping_RC2PNNSN.tsv'
       WITH 
       ( -- DATAFILETYPE = 'char',
         FIELDTERMINATOR = '\t',
         ROWTERMINATOR = '0x0a',
         FIRSTROW = 2,
         BATCHSIZE = 500000
       )

--CMG 12/18/23 - Update to account for FCS Values (File created as part of ingest)
	 TRUNCATE TABLE instaquote_FSC_Data
	 BULK INSERT instaquote_FSC_Data
       FROM '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\Mappings\FCSMappingData.tsv'
       WITH 
       ( 
         FIELDTERMINATOR = '\t',
         ROWTERMINATOR = '0x0a',
         FIRSTROW = 2,
         BATCHSIZE = 500000
       )
    
	alter table #standingOrders
	Add FCSCAGECODE  char(5),
	FCSUOM   char(2)

--ADD FCS Specific information for UOM and CageCode if approrpriate (no Cage Code present on SO, but the REQ on FCS List matches the WIP)

;with cte as (
 select so.reqnno, so.nsn, fsc.uom, fsc.cageCode
 from #standingOrders so
 INNER JOIN instaquote_FSC_Data fsc on fsc.reqno = so.reqnno and fsc.reqpn = so.nsn
 where   DATALENGTH(REPLACE(so.cageCode, CHAR(13), '')) = 0
)
update so
set so.FCSUOM = cte.uom,
so.FCSCAGECODE = cte.cageCode
FROM #standingOrders so 
inner join cte on so.reqnno = cte.reqnno and  so.nsn = cte.nsn



	update #instaQuotes_im_staging set MFRCAGECODE = qs_query.dbo.removeSpecialCharacters(MFRCAGECODE) where ITEMTYPE='PN' and MFRCAGECODE is not null
 	update #instaQuotes_im_staging set VENDORCAGECODE = qs_query.dbo.removeSpecialCharacters(VENDORCAGECODE) where ITEMTYPE='PN' and VENDORCAGECODE is not null   
	update #standingOrders set CAGECODE = qs_query.dbo.removeSpecialCharacters(CAGECODE) where IsTrueNSN='NO' and CAGECODE is not null
	update stg
	set MATCHDESC =
	case 
         when replace(s.NSN,'-','')=replace(flis.MFRPN,'-','') and replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') and s.UOM=stg.UOI and (stg.MFRCAGECODE=s.CAGECODE or stg.VENDORCAGECODE=s.CAGECODE)  then 'Corresponding NSN found for PN in FLIS'
         --
         when ing.NSN  is not null then 'Corresponding NSN found for PN in ingest'
         when cm.NSN is not null then 'Corresponding NSN found for PN in MRO RC2'
         --
         when replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') and stg.MFRCAGECODE=s.CAGECODE and s.UOM=stg.UOI then 'MFRPN, UOI and MFR CAGECODE MATCH'
         when replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') and stg.VENDORCAGECODE=s.CAGECODE and s.UOM=stg.UOI then 'MFRPN, UOI and VENDOR CAGECODE MATCH'
		 --12/19/23 - CMG - New CODE ADDED FOR FCS
		 when replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') and stg.MFRCAGECODE=s.FCSCAGECODE and stg.UOI=s.FCSUOM then 'FCS REQ, MFRPN, UOI and MFR CAGECODE MATCH'
		 when replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') and stg.VendorCAGECODE=s.FCSCAGECODE and stg.UOI=s.FCSUOM then 'FCS REQ, MFRPN, UOI and VENDOR CAGECODE MATCH'
   /*      when replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') and s.UOM=stg.UOI then 'MFRPN and UOI ALONE MATCHES'
		 when replace(s.NSN,'-','')=replace(stg.MFRPN,'-','') then 'MFRPN ALONE MATCHES'*/
	 END,
	 rptOff = s.rptOff

	from #instaQuotes_im_staging stg left outer join #standingOrders s on replace(s.NSN,'-','')=replace(stg.MFRPN,'-','')
    left outer join instaquote_FLISDATA flis on replace(s.NSN,'-','')=replace(flis.MFRPN,'-','') and s.CAGECODE=flis.CAGECODE
    left outer join instaQuotes_ingest ing on replace(s.NSN,'-','')=replace(ing.MFRPNQUOTED,'-','') and ing.ITEMTYPE='NSN'
    left outer join #CombinedMapping_RC2PNNSN cm on replace(s.NSN,'-','')=replace(cm.ManufacturerPN,'-','')

	where stg.ITEMTYPE='PN'and s.IsTrueNSN='NO'

	update stg
    set EXCLUSIONREASON=' Order as NSN instead'
    from #instaQuotes_im_staging stg
    where ITEMTYPE='PN' and MATCHDESC in ('Corresponding NSN found for PN in FLIS', 'Corresponding NSN found for PN in ingest', 'Corresponding NSN found for PN in MRO RC2')

	SET @resultString = @resultString + '||        for Part Numbers when Combinations of MPN,CAGECODE and UOI match ...Done'

	-- Link up Staging with standing orders to update the report office.
	
	--  Add and populate columns to the intermediate staging table.
--    SET @resultString = @resultString + '||   Enriching FOB' 
--   update #instaQuotes_staging set FOB='D' where FOB like '%Dest%'


	--TODO: Include a FLAG. FLAG only if the QUOTEDVSLPP is over a 100% for PN.
	SET @resultString = @resultString + '||    ENRICHING DATA FOR STAGING TABLE ...'
	--Incorporate Last Price Paid
	--LPP is good to have for both NSN and Part Number.  Where to get the last price for part number?  Use NSNDemand for NSN, For PN, find last if available within a year is fine for both NSN and Part Numbers.
   /*
	update stg
	set LPP= LASTPRICEPAID
	from #instaQuotes_staging stg inner join 
	(select NSN, max(LASTPRICEPAID) as LASTPRICEPAID from QS_DASHBOARD.dbo.NSNDEMAND n
	left outer join FL_DB.DBO.FSSICONTRACTS fc on n.CONTNO=fc.CONTNO
	where LASTPRICEPAID > 0  
   -- and (n.CONTNO is null/* or fc.CHANNEL not in ('EUCOM','OCONUS-OTHER')*/) \ -- --Exclude EUCOM/Report Office X for determining Last Price Paid, but then we had discussed that they should not have any contract, which was later dismissed.
    group by NSN) n 
	on stg.NSN=n.NSN 
	where MATCHDESC='EXACT MATCH'  

	update stg
	set LPP= LAST_PRICE__ADV
	from #instaQuotes_staging stg inner join 
	(select MANUPARTNO, max(LAST_PRICE__ADV) as LAST_PRICE__ADV from QS_DASHBOARD.dbo.PARTNO_MASTER where LAST_PRICE__ADV is not NULL group by MANUPARTNO ) p
	on replace(stg.MFRPN,'-','')= replace(p.MANUPARTNO,'-','') 
	where MATCHDESC in ('MFRPN,UOI and CAGECODE MATCH','MFRPN and UOI ALONE MATCHES', 'MFRPN ALONE MATCHES') */

    --Trying to run this against t_reqs and get the latest using item and unit of issue.
    --CMG 7/26/23 - updated the LPP to exclude contracts that begin with X (NDOCOMM/EUCOMM purchases) per Stephen Kinsella

	;with cte as (select PARTNO, UNITISSUE, COSTPRICE as LPP, CONTNO, PODT, rank() over (PARTITION BY PARTNO order by PODT desc) as rn from QS_QUERY.dbo.T_REQUISITIONS t inner join #instaQuotes_im_staging stg on replace(stg.MFRPN,'-','')= replace(t.PARTNO,'-','') and stg.UOI=t.UNITISSUE
        where PARTNO is not NULL and PARTNO <> '' and MATCHDESC in ('FCS REQ, MFRPN, UOI and MFR CAGECODE MATCH','FCS REQ, MFRPN, UOI and VENDOR CAGECODE MATCH','MFRPN, UOI and MFR CAGECODE MATCH','MFRPN, UOI and VENDOR CAGECODE MATCH') and COSTPRICE > 0 
		and (contno is null or contno not like ('X%'))
		)
    update stg
    set stg.LPP = cte.lpp from
    #instaQuotes_im_staging stg inner join cte on replace(stg.MFRPN,'-','')= replace(cte.PARTNO,'-','') and stg.UOI=cte.UNITISSUE
    where cte.rn=1

    ;with cte as (select stg.NSN, UNITISSUE, COSTPRICE as LPP, CONTNO, PODT, rank() over (PARTITION BY stg.NSN order by PODT desc) as rn from QS_QUERY.dbo.T_REQUISITIONS t inner join #instaQuotes_im_staging stg on stg.NSN= t.NSN 
        where t.NSN is not NULL and t.NSN <> '' and  MATCHDESC in ('EXACT MATCH') and COSTPRICE > 0
		and (contno is null or contno not like ('X%'))
	)
    update stg
    set stg.LPP = cte.lpp from
    #instaQuotes_im_staging stg inner join cte on replace(stg.NSN,'-','')= replace(cte.NSN,'-','')
    where cte.rn=1

	--Quoted vs LPP    
	update stg
	set QUOTEDVSLPP = (UNITPRICE-LPP)/LPP*100
    from #instaQuotes_im_staging stg
    where convert(decimal(9,2),LPP)>0


	--Update constants
	update #instaQuotes_im_staging 
	set HORC='', INSTAQUOTE='Y', CO='Steve M. Kinsella', BUYERCODE='B0', COWARRANTLEVEL='$999,999,999'--,QUOTESOURCE='Realtime IQ'
	where MATCHDESC in ('FCS REQ, MFRPN, UOI and MFR CAGECODE MATCH','FCS REQ, MFRPN, UOI and VENDOR CAGECODE MATCH','MFRPN, UOI and MFR CAGECODE MATCH','MFRPN, UOI and VENDOR CAGECODE MATCH', 'EXACT MATCH')

	/* To be excluded based on BNOE aspects from sheet
	if Z is Yes, then 
if Z is No, then AA has to be filled
If AA is Yes,
If AA is No

If J on the Quote Sheet - GSA use says BNOE, then Vendor must input column M on Vendor Input Sheet.
If M=Yes, then N and O can be blank
If M=No, then N has to be Yes
If M=No, then if N = No, then that column cannot be used.

	*/

		
        
	--Incorporate Region
	-- For open market contract (i.e contract indicator of Z), set MOP code to be 86 else T1, and set FAR PART to be 13.203 else 8.405
 --   declare @resultString varchar(max) --For Testing
	BEGIN TRY
		update stg 
		set MOP=case when MAS_OM='OM' then '86' else 'T1'end,
			FARPART=case when MAS_OM='OM' then '13.203' else '8.405' end
		from #instaQuotes_im_staging stg inner join FL_DB.DBO.CMF m on stg.CONTRNUMBER=m.FCONTNO
		where MATCHDESC is not null
		SET @resultString = @resultString + 'Done'
	END TRY
	BEGIN CATCH
		-- 7/30/23 - Save the error information into the @errorString variable instead of directly appending to @resultString
		 SET @errorString = CONCAT('ErrorNumber: ', ERROR_NUMBER(), ' || ',
                              'ErrorState: ', ERROR_STATE(), ' || ',
                              'ErrorSeverity: ', ERROR_SEVERITY(), ' || ',
                              'ErrorProcedure: ', ERROR_PROCEDURE(), ' || ',
                              'ErrorLine: ', ERROR_LINE(), ' || ',
                              'ErrorMessage: ', ERROR_MESSAGE());
		-- Append the error information to @resultString
		SET @resultString = @resultString + '||' + @errorString;

	 --Send EMAIL
	   EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor failed',
			@body = 'Stored Procedure Failed during Staging Update - Review Logs and Instaquote_Status_Log for more information',
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'
			INSERT INTO Instaquote_Status_Log 
			(IQ_Status_Date, IQ_Status_StoredProc, IQ_Status_Python, IQ_Status_Comments)
			Values(getDate(), 'Error at Staging Update', null, @resultString )
		--7/30/23 - CMG - updating error handling
		 -- SELECT
			--ERROR_NUMBER() AS ErrorNumber,
			--ERROR_STATE() AS ErrorState,
			--ERROR_SEVERITY() AS ErrorSeverity,
			--ERROR_PROCEDURE() AS ErrorProcedure,
			--ERROR_LINE() AS ErrorLine,
			--ERROR_MESSAGE() AS ErrorMessage;
			--SET @resultString = @resultString + '||'+ERROR_MESSAGE()+' '+ERROR_LINE()
   SET @emailBody = Concat('Stored Procedure Failed - Review Logs and Instaquote_Status_Log for more information. Details: ' , @resultString)
   EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor failed',
			@body = @emailBody,
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'
	 END CATCH;

	 --Update the final results of the temporary instaquotes_Staging table into actual table
	 --The following two lines are to be run only when there is a change in the columns for #instaQuotes_staging table and then commented
--	 DROP TABLE IF EXISTS instaQuotes_im_staging
--	 select * into instaQuotes_im_staging from #instaQuotes_staging


--CMG 8/21/23 - Updated for removing situations where the quote is greater than 350% of the LPP
--select * from instaquotes_im_staging where 
--			LPP is not null and 
--			quotedvsLPP >350.00
--			and exclusionReason is null

update #instaquotes_im_staging
set ExclusionReason = 'QUOTED PRICE GREATER THAN 350% OF LPP'
where LPP is not null 
	and 
	quotedvsLPP >350.00
	and exclusionReason is null

	--select * from instaquotes_im_staging where exclusionReason is null

--select * from instaquotes_im_staging where
--matchdesc is null and exclusionReason is null
update #instaquotes_im_staging
set ExclusionReason = 'No MATCHING ORDER FOUND'
where MATCHDESC is NULL 
	and exclusionReason is null


	 --The following 3 lines are to be commented when there is a change in the columns for #instaQuotes_staging table and then uncommented.	 
	TRUNCATE TABLE instaQuotes_im_staging
	INSERT INTO instaQuotes_im_staging
	select * from #instaQuotes_im_staging

 -- Get the results and put it into the staging table.
	SET @resultString = @resultString + '||LOADING FINAL DATA INTO THE STAGING TABLE  ... '
	TRUNCATE TABLE instaQuotes_staging
    BEGIN TRY
	    INSERT INTO qs_query.dbo.instaQuotes_staging
    --    drop table if exists instaQuotes_staging
	    SELECT case when MFRPN is null then NSN else MFRPN end as NSN, UNITPRICE, VENDORNAME, CONTRNUMBER, DARO, MINORDTHRESH, VALIDUNTIL, FOB, MOP, RPTOFF, FARPART, HORC, INSTAQUOTE, LPP, CO, BUYERCODE, COWARRANTLEVEL, BUYERNAME, QUOTEDVSLPP, --QUOTESOURCE, 
        MFRNAMEQUOTED, MFRPNQUOTED, ITEMDESC, UOI, case  when MATCHDESC='MFRPN,UOI and MFR CAGECODE MATCH' 
															then MFRCAGECODE
                                                         when MATCHDESC= 'MFRPN,UOI and VENDOR CAGECODE MATCH' 
															then VENDORCAGECODE ELSE MFRCAGECODE 
														END 
													AS CAGECODE, 
														 ITEMTYPE, EXCLUSIONREASON, MATCHDESC
    --    into instaQuotes_staging
	--CMG - 8/17/23 - updated to exclude adding items with an Exclusion Reason
	--CMG - 10/3/23 - Updated to add Excluded items to staging for consideration within the F&R Review. 
	    from qs_query.dbo.instaQuotes_im_staging
	    --where MATCHDESC is not null and EXCLUSIONREASON is null
		where matchdesc is not null and exclusionReason is null 
	    order by NSN

 --       SET @resultString = @resultString + 'Done'
    END TRY
    BEGIN CATCH
	-- 7/30/23 - Save the error information into the @errorString variable instead of directly appending to @resultString
    SET @errorString = CONCAT('ErrorNumber: ', ERROR_NUMBER(), ' || ',
                              'ErrorState: ', ERROR_STATE(), ' || ',
                              'ErrorSeverity: ', ERROR_SEVERITY(), ' || ',
                              'ErrorProcedure: ', ERROR_PROCEDURE(), ' || ',
                              'ErrorLine: ', ERROR_LINE(), ' || ',
                              'ErrorMessage: ', ERROR_MESSAGE());
-- Append the error information to @resultString
    SET @resultString = @resultString + '||' + @errorString;

	   EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor failed',
			@body = 'Stored Procedure Failed during Staging Update two. Review Logs and Instaquote_Status_Log for more information',
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'
		INSERT INTO Instaquote_Status_Log 
		(IQ_Status_Date, IQ_Status_StoredProc, IQ_Status_Python, IQ_Status_Comments)
		Values(getDate(), 'Error at Staging Update two', null, @resultString )
		--7/30/23 - Updating Error handling - data conversion.
   --     SELECT
			--    ERROR_NUMBER() AS ErrorNumber,
			--    ERROR_STATE() AS ErrorState,
			--    ERROR_SEVERITY() AS ErrorSeverity,
			--    ERROR_PROCEDURE() AS ErrorProcedure,
			--    ERROR_LINE() AS ErrorLine,
			--    ERROR_MESSAGE() AS ErrorMessage;

   --SET @resultString = @resultString + '||'+ERROR_MESSAGE()+' '+ERROR_LINE();
   SET @emailBody = Concat('Stored Procedure Failed - Review Logs and Instaquote_Status_Log for more information. Details: ' , @resultString)
   EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor failed',
			@body = @emailBody,
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'
        
   END CATCH

    --select UNITPRICE, LPP, QUOTEDVSLPP, NSN, MFRPNQUOTED from instaQuotes_staging

--    return @resultString
	SET @resultString = REPLACE(@resultString, '||', CHAR(10) + CHAR(13));
	SELECT @resultString AS resultString

 /* Validation
 select s.*, stg.MFRPN, stg.MATCHDESC from
   #standingOrders s left outer join #instaQuotes_staging stg on replace(s.NSN,'-','')=replace(stg.MFRPN,'-','')
	where s.IsTrueNSN='NO'
*/
  -- Example in WIP_DASH where isTrueNSN is a NO but it is infact a NSN  5210009154587
  --Update Log to show success
  INSERT INTO Instaquote_Status_Log 
	(IQ_Status_Date, IQ_Status_StoredProc, IQ_Status_Python, IQ_Status_Comments)
	Values(getDate(), 'SUCCESSFULLY COMPLETED', null, @resultString )
--Send email confirming completion
     EXEC msdb.dbo.sp_send_dbmail
			@recipients='curtis.gathright@gsa.gov;instaquote.support.team@gsa.gov;',
			--@recipients='curtis.gathright@gsa.gov,
			@from_address='instaquote-no-reply@gsa.gov',
			@subject = 'Stored Procedure: sp_instaQuoteProcessor completed successfully',
			@body = 'Stored Procedure sp_instaQuoteProcessor Completed Successfully.',
			@body_format = 'TEXT',
			@profile_name = 'SQLAdmin'
END

/* Two views created
DROP VIEW IF EXISTS v_instaQuoteNSN
CREATE VIEW v_instaQuoteNSN AS  
	SELECT NSN,	ITEMDESC, UOI,	PKGQTY, UNITPRICE,	VENDORNAME,	MAS_OM, CONTRNUMBER,	DARO,	MINORDTHRESH,	VALIDUNTIL,	FOB,	MFRNAMEQUOTED,	MFRPNQUOTED,	COO, BNOE,	ISBRANDNAMEPN,	DOCUMENTATION,	ACCEPTABLESUBSTITUTE,	QUOTEDATE,ITEMTYPE 
	FROM instaQuotes_ingest;  
GO
DROP VIEW IF EXISTS v_instaQuotePN
CREATE VIEW v_instaQuotePN AS 
SELECT MFRNAME,	MFRCAGECODE, VENDORCAGECODE, SAMMFRNAME,MFRPN,	UOI,	PKGQTY,	UNITPRICE,	VENDORNAME,	MAS_OM,CONTRNUMBER,	DARO,	MINORDTHRESH,	VALIDUNTIL,	FOB,	ITEMDESC,		COO, QUOTEDATE, ITEMTYPE
	FROM instaQuotes_ingest;  
GO
*/

/* For Testing Purposes
BULK INSERT v_instaQuoteNSN FROM '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\Complete Packaging & Shipping Supplies_NSN.tsv' WITH 
		( 
			DATAFILETYPE = 'char',
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			BATCHSIZE = 500000,
			FORMATFILE='\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\FormatFiles\r6NSNFormat.xml'
		)
BULK INSERT v_instaQuotePN FROM '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\Partsmaster_PN.tsv' WITH 
		( 
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			BATCHSIZE = 500000,
			FORMATFILE = '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\FormatFiles\r6PNFormat.xml'
		)

        --Once you locate the file that has issue do this to see the files...there would be couple of error files in below directory that would be very useful.
BULK INSERT v_instaQuoteNSN FROM '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\MRO_PN_TSVS\Complete Packaging & Shipping Supplies_NSN.tsv' WITH 
		( 
			DATAFILETYPE = 'char',
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '\n', --'0x0a',
			FIRSTROW = 2,
			BATCHSIZE = 500000,
			FORMATFILE='\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\FormatFiles\r6NSNFormat.xml',       
            MAXERRORS = 100,
            ERRORFILE = '\\E04TCM-GSASQL03\SQLFTP$\DATALOAD\Automation\R6_Instaquote\errors_log'
		)

declare @nsnfiles varchar(max), @pnfiles varchar(max),@fileString varchar(max),@returnString varchar(max)
set @nsnfiles='Central Power Systems & Services_NSN.tsv,Complete Packaging & Shipping Supplies_NSN.tsv,DMC_NSN.tsv,Giga_NSN.tsv,Inserts & Kits_NSN.tsv,Kaufman_NSN.tsv,Kipper_NSN.tsv,Mid-Continent Machining_NSN.tsv,Premier_NSN.tsv,Timber Edge Machine_NSN.tsv,Urenda_NSN.tsv,WTC_NSN.tsv'
set @pnfiles='Chester Supply_PN.tsv,Divine Imaging_PN.tsv,Emcor Crenlo_PN.tsv,Kimball Midwest_PN.tsv,KRCampbell Hot and Cold Plumbing_PN.tsv,Laine Industries_PN.tsv,MSC_PN.tsv,Noble Supply_PN.tsv,Partsmaster_PN.tsv,San Antonio Lighthouse_PN.tsv,Supply Chimp_PN.tsv,W.W.Grainger_PN.tsv'
--set @fileString=@nsnfiles+','+@pnfiles
--set @fileString=@pnfiles
--set @fileString=@nsnfiles
--print @fileString
set @fileString='Central Power Systems & Services_NSN.tsv'
exec QS_QUERY.dbo.sp_instaQuoteProcessor @fileString, '',@returnString OUTPUT


select vendorname, mfrname,count(*) as numofQuotes from instaQuotes_ingest group by mfrname, VENDORNAME order by VENDORNAME

select vendorname, itemtype, count(*) as numofQuotes from qs_query.dbo.instaQuotes_ingest group by VENDORNAME, ITEMTYPE order by itemtype, VENDORNAME

select * from instaQuotes_staging

sp_columns instaQuotes_ingest
sp_columns instaQuotes_staging

select vendorname, count(*) from instaQuotes_ingest group by vendorname
*/
