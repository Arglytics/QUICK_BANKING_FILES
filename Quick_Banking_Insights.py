from required_functions import *

#====================== DEFINE CUSTOM QUERIES FOR QUICK BANKING KPIS  ===========================

#CUSTOMER DETAILS

customer_demographic_query = """

SELECT [CUSTOMER_ID]
      ,[CUSTOMER_NAME]
      ,[GENDER]
      ,[OCCUPATION_ID]
      ,[SECTOR_CODE]     
      ,[CUSTOMER_OPEN_DATE]
      ,[BRANCH_ID]
      ,[PHONE]
      ,[QUICK_BANKING_FLAG]     
  FROM [DASH20].[dbo].[dimCUSTOMERS] WHERE [CUSTOMER_ID] IN ({})


"""
customer_region_branch_query = """
select C.CUSTOMER_ID, BRANCH_NAME,REGION_NAME from dimCUSTOMERS C
 LEFT OUTER JOIN dimBRANCHES B ON B.BRANCH_ID=C.BRANCH_ID
 LEFT OUTER JOIN dimREGIONS R ON R.REGION_CODE=B.REGION_CODE
"""

#ALL QB ONBOARDED CUSTOMERS
all_onboarded ="""
SELECT Onboarding.*,customers.PHONE FROM
(

SELECT DISTINCT [Number Customer] as CUSTOMER_ID,
Trust_Factor_Flag AS STATUS,
min(Trust_Factor_Reg_Date) OVER (PARTITION BY [Number Customer]) AS TRUSTFACTOR_REGISTERED_DATE,
min([MUC Contract Creation Date]) OVER (PARTITION BY [Number Customer]) AS ONBOARDED_DATE,
CONVERT(VARCHAR(50), [MUC Contract Number]) AS USERNAME
--[MUC Contract Number] AS USERNAME
FROM [DASH20].[dbo].QUICK_CONNECT_NEW
group by [MUC Contract Creation Date],[MUC Contract Number],[Number Customer],Trust_Factor_Flag,Trust_Factor_Reg_Date

UNION ALL


SELECT distinct CUSTOMER_ID,STATUS,TRUSTFACTOR_REGISTERED_DATE,
min(ONBOARDED_DATE)  OVER (PARTITION BY CUSTOMER_ID) ONBOARDED_DATE,
CONVERT(VARCHAR(50), CONTRACT_NUMBER) AS USERNAME
--CONTRACT_NUMBER AS USERNAME
FROM [DASH20].[dbo].QuickOnBoarding
group by ONBOARDED_DATE,CONTRACT_NUMBER,CUSTOMER_ID,STATUS,TRUSTFACTOR_REGISTERED_DATE
) Onboarding

LEFT JOIN 
[DASH20].[dbo].dimCUSTOMERS customers ON Onboarding.CUSTOMER_ID = customers.CUSTOMER_ID

"""

#ACCOUNT STATUS

account_status = """
SELECT 
	DISTINCT A.CUSTOMER_ID, A.ACCOUNT_STATUS,AC.ACCOUNT_OPEN_DATE,B.BRANCH_NAME BRANCH
FROM 
	[DASH20].[dbo].ACCOUNT_ACTIVITY A
	LEFT JOIN [DASH20].[dbo].dimACCOUNTS AC ON A.CUSTOMER_ID =  AC.CUSTOMER_ID
	LEFT OUTER JOIN [DASH20].[dbo].dimBRANCHES B ON B.BRANCH_ID=AC.BRANCH_ID
WHERE
	REPORTING_DATE = DATEADD(day, -1, convert(date, GETDATE()))
	AND A.CLOSURE_FLAG='N' 

"""


#LOGGED IN LAST 150 DAYS -- 5-MONTHS PERIOD SO THAT WE CAN GET NEW USERS THIS MONTH VS NEW USERS LAST MONTH
last_150_day_login = """
DECLARE @MAX_DATE AS DATE SELECT @MAX_DATE = MAX(EventTime)  FROM [DASH20].[dbo].[QB_NON_VALUE_TRANSACTIONS] WHERE Transaction_Type = 'LoginTransaction' AND Application !='FRONTOFFICE'
SELECT	
	UserName,Application Device,
	EventMessage login_status,
	EventTime login_time,
	DATEDIFF(day,CONVERT(DATE, EventTime),@MAX_DATE)  days_since_last_login,
	MONTH(EventTime) login_month
FROM 
	[DASH20].[dbo].[QB_NON_VALUE_TRANSACTIONS]
WHERE 
	Transaction_Type = 'LoginTransaction' AND Application !='FRONTOFFICE' 
	AND CONVERT(DATE, EventTime) BETWEEN CONVERT(DATE, DATEADD(DAY, -150, @MAX_DATE)) AND CONVERT(DATE, @MAX_DATE)

"""
#LOGGED IN LAST 90 DAYS 
last_90_day_login = """
SELECT 		
	UserName,Application Device,
	EventMessage login_status,
	EventTime login_time	 
FROM 
	[DASH20].[dbo].[QB_NON_VALUE_TRANSACTIONS]
WHERE 
	Transaction_Type = 'LoginTransaction' AND Application !='FRONTOFFICE'
	AND CONVERT(DATE, EventTime) BETWEEN CONVERT(DATE, DATEADD(DAY, -90, GETDATE())) AND CONVERT(DATE, GETDATE())

"""


first_login_query = """

SELECT 	
	UserName,Application Device,
	EventMessage First_login_status,
	min(EventTime) First_Login_Attempt	 
FROM 
	[DASH20].[dbo].[QB_NON_VALUE_TRANSACTIONS]
WHERE 
	Transaction_Type = 'LoginTransaction' AND Application !='FRONTOFFICE'
GROUP BY UserName,Application,EventMessage

"""
last_login_query = """

SELECT 	
	UserName,Application Device,
	EventMessage Last_login_status,
	max(EventTime) Last_Login_Attempt	 
FROM 
	[DASH20].[dbo].[QB_NON_VALUE_TRANSACTIONS]
WHERE 
	Transaction_Type = 'LoginTransaction' AND Application !='FRONTOFFICE'
GROUP BY UserName,Application,EventMessage

"""


    
all_transactions = """
SELECT *,
	CASE WHEN Status='Executed' then 'Success' ELSE 'Failed' end as TRANSACTION_STATUS
FROM 
	[DASH20].[dbo].QB_TRANSACTIONS  
WHERE
	Date >=  DATEADD(Month, DATEDIFF(month, 0, DATEADD(m, -6, GETDATE())),0)
    	
"""


account_status_query = """
SELECT 
	DISTINCT A.CUSTOMER_ID, A.ACCOUNT_STATUS,AC.ACCOUNT_NUMBER,AC.ACCOUNT_OPEN_DATE,
	C.NAME, C.CUSTOMER_TYPE,  C.GENDER,C.MARITAL_STATUS,C.DATE_OF_BIRTH,C.TOWN,C.DISTRICT, C.REGION,B.BRANCH_NAME BRANCH
FROM 
	[DASH20].[dbo].ACCOUNT_ACTIVITY A
	LEFT JOIN [DASH20].[dbo].dimACCOUNTS AC ON A.CUSTOMER_ID =  AC.CUSTOMER_ID	
	LEFT JOIN [DASH20].[dbo].CUSTOMER_DETAILS C ON A.CUSTOMER_ID = C.CUSTOMER_ID
	LEFT OUTER JOIN [DASH20].[dbo].dimBRANCHES B ON B.BRANCH_ID= AC.BRANCH_ID
WHERE
	REPORTING_DATE = DATEADD(day, -1, convert(date, GETDATE()))
	AND A.CLOSURE_FLAG='N'
"""



QB_CUSTOMERS_AND_ACCOUNTS_query = """

SELECT DISTINCT P.CUSTOMER_ID,P.ONBOARDED_DATE,AA.ACCOUNT_OPEN_DATE,AA.ACCOUNT_NUMBER FROM
(
SELECT T.Customer_ID, R.REGION_NAME,  M.BRANCH_ID,  T.ONBOARDED_DATE,W.FIRST_DATE_TO_TRANSACT_ON_QB,W.volume,
CASE WHEN  w.volume > 0 THEN 'ACTIVE' ELSE 'DORMANT' END AS QB_STATUS,
CASE WHEN M.TRANSACTING='Y' THEN 'Y' ELSE 'N' END AS TRANSACTING,T.Trust_Factor_Flag,
T.Trust_Factor_Reg_Date,T.CONTRACT_NUMBER,N.LAST_LOGIN_90

FROM (
SELECT DISTINCT [Number Customer] as CUSTOMER_ID,[MUC Contract Creation Date] AS ONBOARDED_DATE,
Trust_Factor_Flag,Trust_Factor_Reg_Date,CONVERT(VARCHAR(50), [MUC Contract Number]) AS CONTRACT_NUMBER
FROM QUICK_CONNECT_NEW Q
INNER JOIN dimACCOUNTS A ON A.CUSTOMER_ID=Q.[Number Customer]
where A.CLOSURE_FLAG='N' 

UNION ALL

SELECT Q.CUSTOMER_ID,ONBOARDED_DATE,
CASE WHEN TRUSTFACTOR_REGISTERED_DATE IS NOT NULL THEN 'Registered' else 'Not-registered' end as Trust_Factor_Flag,
TRUSTFACTOR_REGISTERED_DATE as Trust_Factor_Reg_Date,CONVERT(VARCHAR(50), CONTRACT_NUMBER) AS CONTRACT_NUMBER
FROM QuickOnBoarding Q
INNER JOIN dimACCOUNTS A ON A.CUSTOMER_ID=Q.CUSTOMER_ID
where A.CLOSURE_FLAG='N'


) T
LEFT OUTER JOIN (
		SELECT CUSTOMER_ID,count(Customer_ID) as volume, ACCOUNT_NUMBER, MIN(Date) AS FIRST_DATE_TO_TRANSACT_ON_QB,OPERATION 
		FROM QB_TRANSACTIONS 
		WHERE CONVERT(DATE,Date,112) >= (SELECT DATEADD(D, -90, CONVERT(DATE, GETDATE())))  AND Status ='Executed'
		GROUP BY CUSTOMER_ID,ACCOUNT_NUMBER,Date,OPERATION
		HAVING COUNT(*) > 0
)W ON W.CUSTOMER_ID = T.CUSTOMER_ID
--GROUP BY T.CUSTOMER_ID,T.ONBOARDED_DATE,W.volume
--)K
LEFT OUTER JOIN (
		SELECT ACCOUNT_NUMBER AS ACTIVE_ACCTS, CUSTOMER_ID,'Y' AS TRANSACTING,BRANCH_ID FROM ACCOUNT_ACTIVITY
		WHERE REPORTING_DATE = DATEADD(day, -1, convert(date, GETDATE()))
		AND ACCOUNT_STATUS = 'A' AND CLOSURE_FLAG <>'Y'
		GROUP BY CUSTOMER_ID,ACCOUNT_NUMBER,BRANCH_ID
		--HAVING COUNT(ACCOUNT_NUMBER) > 0
)M ON M.CUSTOMER_ID = T.CUSTOMER_ID
INNER JOIN dimBRANCHES B ON B.BRANCH_ID=M.BRANCH_ID
INNER JOIN dimREGIONS R ON R.REGION_CODE=B.REGION_CODE
LEFT JOIN (
SELECT UserName,COUNT(DISTINCT UserName) AS LAST_LOGIN_90 FROM QB_NON_VALUE_TRANSACTIONS 
WHERE Transaction_Type = 'LoginTransaction' 
	AND CONVERT(DATE, EventTime) BETWEEN CONVERT(DATE, DATEADD(DAY, -90, GETDATE())) AND CONVERT(DATE, GETDATE())
    GROUP BY UserName
)N ON CONVERT(VARCHAR(50), T.CONTRACT_NUMBER) = CONVERT(VARCHAR(50), N.UserName)
--ORDER BY LAST_LOGIN_90 DESC
) P
INNER JOIN dimACCOUNTS AA ON AA.CUSTOMER_ID=P.CUSTOMER_ID
WHERE CLOSURE_FLAG ='N'	
"""

Transformed_contacted_customers_query=""" 
SELECT P.*, L.PERSONA
FROM (
 SELECT *, CASE WHEN A.[CUST ID]
 IN  (  select CUSTOMER_ID from UPDATED_QB_UTILIZATION_LEADS)
 THEN 'TRANSFORMED' ELSE 'NOT_TRANSFORMED'
END AS FOLLOW_UP_STATUS
FROM [TEST_DB].[dbo].[QB_FOLLOW_UP_LATEST] A
) P
LEFT OUTER JOIN UPDATED_QB_UTILIZATION_LEADS L ON L.CUSTOMER_ID=P.[CUST ID] """

Transformed_contacted_customers_latest_query=""" 


select P.*, CASE WHEN P.[CUST ID] IN (SELECT DISTINCT T.CUSTOMER_ID FROM QB_NON_VALUE_TRANSACTIONS Q
  LEFT JOIN (

SELECT DISTINCT [Number Customer] as CUSTOMER_ID,CONVERT(VARCHAR(50), [MUC Contract Number]) AS CONTRACT_NUMBER
FROM QUICK_CONNECT_NEW
UNION ALL
SELECT CUSTOMER_ID
,CONVERT(VARCHAR(50), CONTRACT_NUMBER) AS CONTRACT_NUMBER
FROM QuickOnBoarding
) T ON CONVERT(VARCHAR(50), T.CONTRACT_NUMBER) = CONVERT(VARCHAR(50), Q.UserName)
WHERE Transaction_Type = 'LoginTransaction' 
	AND CONVERT(DATE, EventTime) BETWEEN CONVERT(DATE, DATEADD(DAY, -30, GETDATE())) AND CONVERT(DATE, GETDATE())
    GROUP BY UserName , T.CUSTOMER_ID
) THEN 'TRANSFORMED' ELSE 'NOT_TRANSFORMED' END AS NEW_STATUS, K.MAX_LOG_IN_DATE , case when P.[CUST ID]  IN (
select  customer_id FROM QB_TRANSACTIONS
WHERE 
CONVERT(DATE, Date) BETWEEN CONVERT(DATE, DATEADD(DAY, -30, GETDATE())) AND CONVERT(DATE, GETDATE())) THEN 'Logged_in_and_transacated' ELSE 'not_transacted' 
END AS DID_TRANSACT , L.MAX_TRAN_DATE , TR.TRANSACTION_DATE,     L.Type,L.Account_Number, L.TRANSACTION_AMOUNT , TR.CHARGE_AMOUNT , TR.NARRATIVE

FROM [TEST_DB].[dbo].[QB_FOLLOW_UP_LATEST]  P


LEFT OUTER JOIN (
 SELECT T.CUSTOMER_ID,   UserName,MAX(CONVERT(DATE, EventTime)) AS MAX_LOG_IN_DATE FROM QB_NON_VALUE_TRANSACTIONS Q
  LEFT JOIN (

  SELECT DISTINCT [Number Customer] as CUSTOMER_ID,CONVERT(VARCHAR(50), [MUC Contract Number]) AS CONTRACT_NUMBER
FROM QUICK_CONNECT_NEW
UNION ALL
SELECT CUSTOMER_ID
,CONVERT(VARCHAR(50), CONTRACT_NUMBER) AS CONTRACT_NUMBER
FROM QuickOnBoarding
) T ON CONVERT(VARCHAR(50), T.CONTRACT_NUMBER) = CONVERT(VARCHAR(50), Q.UserName)
WHERE Transaction_Type = 'LoginTransaction' 
	AND CONVERT(DATE, EventTime) BETWEEN CONVERT(DATE, DATEADD(DAY, -90, GETDATE())) AND CONVERT(DATE, GETDATE())
    GROUP BY UserName , T.CUSTOMER_ID ) K ON K.CUSTOMER_ID = P.[CUST ID]
LEFT OUTER JOIN (
select  customer_id,MAX(CONVERT(DATE, Date)) MAX_TRAN_DATE, Type,TRANSACTION_AMOUNT, Account_Number FROM QB_TRANSACTIONS
WHERE  CONVERT(DATE, Date) BETWEEN CONVERT(DATE, DATEADD(DAY, -30, GETDATE())) AND CONVERT(DATE, GETDATE())
GROUP BY  customer_id,Type,TRANSACTION_AMOUNT,Account_Number) L ON L.Customer_ID=P.[CUST ID]
LEFT OUTER JOIN (SELECT ACCOUNT_NUMBER, TRANSACTION_AMOUNT AS CHARGE_AMOUNT,NARRATIVE,TRANSACTION_DATE FROM TRANSACTIONS
WHERE GL_CODE = '26500' and DEBIT_CREDIT='D' AND NARRATIVE LIKE '%Charges%' AND TRANSACTION_SERIAL_NUMBER=3
) TR ON TR.ACCOUNT_NUMBER = L.Account_Number AND CAST(TR.TRANSACTION_DATE AS DATE) = L.MAX_TRAN_DATE
"""


#====================== END CUSTOM QUERIES FOR QUICK BANKING KPIS  ===========================

print("|-- "+"="*70)
print("|-- Get login activity")
#FIRST LOGIN
first_login = [ ]
for chunk in pd.read_sql(first_login_query,connection,chunksize=100000):
    first_login.append(chunk)
first_login = pd.concat(first_login)
#STANDARDIZE COLUMNS
first_login.columns = [x.upper() for x in first_login.columns]
first_login["USERNAME"] = first_login["USERNAME"].str.strip().str.lower()
start = first_login.merge(first_login.groupby("USERNAME")["FIRST_LOGIN_ATTEMPT"].min().reset_index().rename({"FIRST_LOGIN_ATTEMPT":"FIRST_LOGIN"},axis=1),on="USERNAME")
start["IS_FIRST_LOGIN"] = np.where(start.FIRST_LOGIN_ATTEMPT == start.FIRST_LOGIN,1,0)
start = start[start.IS_FIRST_LOGIN>0].reset_index(drop=True).rename({"DEVICE":"FIRST_DEVICE"},axis=1).drop("IS_FIRST_LOGIN",axis=1)
print("|-- First login")
#LAST LOGIN
last_login = [ ]
for chunk in pd.read_sql(last_login_query,connection,chunksize=200000):
    last_login.append(chunk)
last_login = pd.concat(last_login)
last_login.columns = [x.upper() for x in last_login.columns]
last_login["USERNAME"] = last_login["USERNAME"].str.strip().str.lower()
last_login["LAST_LOGIN_ATTEMPT"] = pd.to_datetime(last_login["LAST_LOGIN_ATTEMPT"])
last_login["USERNAME"] = last_login["USERNAME"].map(str).str.strip().str.lower()
max_date = last_login["LAST_LOGIN_ATTEMPT"].max()
today = dt.datetime.now()
last_login["LAST_LOGIN_WINDOW_IN_DAYS"] = (today -  last_login["LAST_LOGIN_ATTEMPT"])/np.timedelta64(1,'D')
last_login["LAST_LOGIN_WINDOW_IN_DAYS"] = round(last_login["LAST_LOGIN_WINDOW_IN_DAYS"],0).map(int)


#IDENTIFY ACTIVE USERS: LOGIN WITHIN 90 DAYS
last_login["CUSTOMER_STATUS"] = np.where(last_login.LAST_LOGIN_WINDOW_IN_DAYS<91,"Active","Inactive")
end = last_login.merge(last_login.groupby("USERNAME")["LAST_LOGIN_ATTEMPT"].max().reset_index().rename({"LAST_LOGIN_ATTEMPT":"LAST_LOGIN"},axis=1),on="USERNAME")
end["IS_LAST_LOGIN"] = np.where(end.LAST_LOGIN_ATTEMPT == end.LAST_LOGIN,1,0)
end = end[end.IS_LAST_LOGIN>0].reset_index(drop=True).rename({"DEVICE":"LAST_DEVICE"},axis=1).drop("IS_LAST_LOGIN",axis=1)
print("|-- Last login")
login_activity = [ ]
for chunk in pd.read_sql(last_150_day_login,connection,chunksize=250000):
    login_activity.append(chunk)
all_activity = pd.concat(login_activity) #KEEP ALL 180-DAY LOGIN RECORDS
all_activity.columns =  [x.upper() for x in all_activity.columns]
all_activity["USERNAME"] = all_activity["USERNAME"].map(str).str.strip().str.lower()
login_activity =  all_activity[all_activity.DAYS_SINCE_LAST_LOGIN<91] #GET ACTIVE CUSTOMERS BASED ON LAST 90-DAY LOGIN
login_activity["CHANNEL"] = login_activity["DEVICE"].apply(get_channel_from_device)
from dateutil.relativedelta import relativedelta
five_month_activity = all_activity.copy()
five_month_activity["LOGIN_DATE"] = five_month_activity["LOGIN_TIME"].dt.strftime("%Y-%m-%d")
max_date = pd.to_datetime(five_month_activity.LOGIN_TIME.max().strftime("%Y-%m-%d"))
last_90_days_from_max_date = max_date + relativedelta(days=-90)
month_max_date = max_date.month
current_month = dt.datetime.now().month
last_month = max_date + relativedelta(months=-1)
start_last_month = pd.to_datetime(last_month.strftime("%Y-%m")+"-01")
last_90_days_from_last_month = last_month + relativedelta(days=-90)
before_last_month = max_date + relativedelta(months=-2)
five_month_activity["THIS MONTHS ACTIVITY"] = np.where((five_month_activity.LOGIN_MONTH - current_month) == 0,1,0)
five_month_activity["ACTIVE_THIS_MONTH"] = np.where((five_month_activity.LOGIN_TIME >= last_90_days_from_max_date ) & (five_month_activity.LOGIN_TIME <= max_date),1,0)
five_month_activity["ACTIVE_LAST_MONTH"] = np.where((five_month_activity.LOGIN_TIME >= last_90_days_from_last_month ) & (five_month_activity.LOGIN_TIME <= last_month),1,0)
no_of_months = five_month_activity[five_month_activity.LOGIN_STATUS.str.lower()=='success'].groupby("USERNAME")["LOGIN_MONTH"].nunique().reset_index().rename({"LOGIN_MONTH":"NO_MONTHS"},axis=1)
active_this_month = five_month_activity[(five_month_activity.ACTIVE_THIS_MONTH==1)&(five_month_activity.LOGIN_STATUS.str.lower()=='success')].groupby("USERNAME")["LOGIN_TIME"].max().reset_index().rename({"LOGIN_TIME":"LAST_90_DAYS_THIS_MONTH"},axis=1)
active_last_month = five_month_activity[(five_month_activity.ACTIVE_LAST_MONTH==1)&(five_month_activity.LOGIN_STATUS.str.lower()=='success')].groupby("USERNAME")["LOGIN_TIME"].max().reset_index().rename({"LOGIN_TIME":"LAST_90_DAYS_LAST_MONTH"},axis=1)
last_five_months_summary =  active_this_month.merge(active_last_month,how="outer",on="USERNAME").merge(no_of_months,on="USERNAME")

#LOGIN FAILURE RATE
login_summary = login_activity.groupby("USERNAME".split())["LOGIN_STATUS"].value_counts().unstack().reset_index().fillna(0)
login_summary["Failed Success".split()] = login_summary["Failed Success".split()].apply(lambda x: x.map(int))
login_summary["%_Login_Failure"] = (login_summary["Failed"]) / (login_summary["Failed Success".split()].sum(axis=1))
login_summary["%_Login_Failure"] = round(login_summary["%_Login_Failure"]*100,0);login_summary["%_Login_Failure"] = login_summary["%_Login_Failure"].astype(int)

#CHANNEL ACTIVITY
channel_summary =  login_activity.groupby("USERNAME")["CHANNEL"].value_counts().unstack().reset_index().fillna(0)
channel_summary.iloc[:,1:] = channel_summary.iloc[:,1:].apply(lambda x: x.map(int))
def is_a_channel(x):
    if x >= 1:
        return 1
    else:
        return 0
channels = channel_summary.iloc[:,1:]
for col in channels.columns:
    channels[col] = channels[col].apply(is_a_channel)
channels["No_of_Channels"] =  channels.sum(axis=1)
channel_summary = pd.concat([channel_summary,channels["No_of_Channels"]],axis=1)
options = "Web App USSD".split()
channel_summary["Total_Logins"] = channel_summary[options].sum(axis=1)
channel_summary["Preferred_Channel"] = channel_summary[options].idxmax(axis=1)

#PREFERRED DEVICE
device_summary =  login_activity.groupby("USERNAME")["DEVICE"].value_counts().unstack().reset_index().fillna(0)
device_summary.iloc[:,1:] = device_summary.iloc[:,1:].apply(lambda x: x.map(int))
device_summary["Preferred_Device"] = device_summary.iloc[:,1:].idxmax(axis=1)
device_summary = device_summary["USERNAME Preferred_Device".split()]

#FINAL MERGE
login_master = login_summary["USERNAME %_Login_Failure".split()].merge(channel_summary,on="USERNAME").merge(device_summary,
                on="USERNAME").sort_values("Total_Logins %_Login_Failure".split(),ascending=[False,True]).reset_index(drop=True).merge(start,on="USERNAME").merge(end,on="USERNAME")
login_master["Preferred_Device"] = login_master["Preferred_Device"].apply(get_device_type)
login_master["Login_Failure_Rate"]  = login_master["%_Login_Failure"]
login_master["Login_Success_Rate"]  = (100 - login_master["Login_Failure_Rate"] )
login_master = login_master.drop("%_Login_Failure",axis=1).rename({"Total_Logins":"Login_Attempts"},axis=1).drop("FIRST_LOGIN LAST_LOGIN".split(),axis=1)
login_master.columns = [x.upper() for x in login_master.columns]

print("|-- Get all onboarded customers.")
onboarded = [ ]
for chunk in pd.read_sql(all_onboarded,connection,chunksize=100000):
    onboarded.append(chunk)
onboarded = pd.concat(onboarded)
#STANDARDIZE USERNAME
onboarded["USERNAME"] = onboarded["USERNAME"].map(str).str.strip().str.lower()

print("|-- Merge onboarding and login data")
#LOGIN AND ONBOARDING
activity_overview = onboarded.merge(login_master.query("LOGIN_SUCCESS_RATE>0"),on="USERNAME",how="outer")

print("|-- Introducing transaction data")
txns = [ ]
for chunk in pd.read_sql(all_transactions,connection,chunksize=100000):
    txns.append(chunk)
txns = pd.concat(txns).rename({"Contract_Number":"Username"},axis=1)
txns.columns = [x.upper() for x in txns.columns]
txns["USERNAME"] = txns["USERNAME"].map(str).str.strip().str.lower()
#CHECK TXNS DONE BY CUSTOMER
txns["DATE"] = pd.to_datetime(txns["DATE"])
#GET TRANSACTING PERIOD IN MONTHS
# txns["PERIOD"] = rd(max_date, txns["DATE"]).months#/np.timedelta64(1, 'M')
# txns["PERIOD"] = round(txns["PERIOD"],0).map(int)
txns['PERIOD']=pd.to_numeric(max_date.strftime('%m'),errors='coerce')-txns["DATE"].dt.strftime('%m').astype(int)
txns['PERIOD']=pd.to_numeric(txns['PERIOD'].map(str).apply(lambda x: str(x).replace('-','') if x.__contains__('-') else x),errors='coerce')

#GET UNIQUE USERS WHO ARE TRANSACTING
transacting_users = txns["USERNAME CUSTOMER_ID".split()].drop_duplicates().reset_index(drop=True).rename({"CUSTOMER_ID":"TRANSACTING_CIF"},axis=1)
transacting_active= activity_overview.merge(transacting_users,on="USERNAME",how="left")
transacting_active["CUSTOMER_ID"] =  np.where(transacting_active.CUSTOMER_ID.isna() & transacting_active.TRANSACTING_CIF.notna(), transacting_active.TRANSACTING_CIF, transacting_active.CUSTOMER_ID)
transacting_active =  transacting_active.sort_values("CUSTOMER_ID").reset_index(drop=True)
#MERGE ONBOARDING AND CUSTOMER TRANSACTION DATA
onboarding_metadata =onboarded.copy().drop("USERNAME",axis=1).drop_duplicates()
rename =  {"STATUS":"STATUS_","TRUSTFACTOR_REGISTERED_DATE":"TRUSTFACTOR","ONBOARDED_DATE":"ONBOARDED","PHONE":"CONTACT"}
drop = "STATUS	TRUSTFACTOR_REGISTERED_DATE	ONBOARDED_DATE PHONE TRANSACTING_CIF".split()
onboarding_metadata = onboarding_metadata.rename(rename,axis=1)
transacting_active_ = transacting_active.merge(onboarding_metadata,on="CUSTOMER_ID",how="left").drop(drop,axis=1)
print("|-- Introducing the Account Status")
accounts = [ ]
for chunk in pd.read_sql(account_status_query,connection,chunksize=100000):
    accounts.append(chunk)
accounts = pd.concat(accounts)
no_of_accounts = accounts.groupby("CUSTOMER_ID")["ACCOUNT_OPEN_DATE"].nunique().reset_index().rename({"ACCOUNT_OPEN_DATE":"NO_ACCOUNTS"},axis=1)
accounts["ACCOUNT_OPEN_DATE"] =  pd.to_datetime(accounts["ACCOUNT_OPEN_DATE"])
first_account = accounts.groupby("CUSTOMER_ID")["ACCOUNT_OPEN_DATE"].min().reset_index().sort_values("CUSTOMER_ID ACCOUNT_OPEN_DATE".split(),ascending=[False,True]).drop_duplicates("CUSTOMER_ID",keep="first").reset_index(drop=True)
first_account = first_account.merge(accounts["BRANCH CUSTOMER_ID ACCOUNT_OPEN_DATE".split()],on="CUSTOMER_ID ACCOUNT_OPEN_DATE".split(),how="left").rename({"ACCOUNT_OPEN_DATE":"JOINED_DFCU"},axis=1).sort_values("JOINED_DFCU").reset_index(drop=True)
acc_summary   = first_account.merge(no_of_accounts,on="CUSTOMER_ID")

account_status = accounts.groupby("CUSTOMER_ID")["ACCOUNT_STATUS"].value_counts().unstack().reset_index().fillna(0)
account_status.iloc[:,1:] = account_status.iloc[:,1:].apply(lambda x: x.map(int))
account_status["STATUS"] = np.where(account_status.A>0,"A","D")
acc_summary = acc_summary.merge(account_status["CUSTOMER_ID STATUS".split()],on="CUSTOMER_ID")

demographic_summary = accounts["CUSTOMER_ID NAME CUSTOMER_TYPE GENDER MARITAL_STATUS DATE_OF_BIRTH TOWN DISTRICT REGION".split()].drop_duplicates().reset_index(drop=True)
account_summary  = acc_summary.merge(demographic_summary, on="CUSTOMER_ID",how="left")
#FORMAT BRANCH
account_summary["BRANCH"] = account_summary["BRANCH"].str.upper().str.replace("BRANCH","").str.strip()
print("|-- Filtering - Focus on only Active Bank customers")
#ALL ACTIVE BANK CUSTOMERS
active_bank_customers_list = account_summary[account_summary.STATUS=='A']["CUSTOMER_ID"].unique().tolist()
transacting_active_["IS_ACTIVE_BANK_CUSTOMER"] =  np.where(transacting_active_.CUSTOMER_ID.isin(active_bank_customers_list),1,0)
active_customers  = transacting_active_.merge(account_summary,on="CUSTOMER_ID").query("STATUS=='A'").reset_index(drop=True)
active_customers.columns = [x.upper() for x in active_customers.columns]
active_customers["DAYS_SINCE_LAST_ATTEMPT"] = round((max_date - active_customers["LAST_LOGIN_ATTEMPT"])/np.timedelta64(1,'D'),0)
numericals = "APP USSD WEB	NO_OF_CHANNELS LOGIN_ATTEMPTS DAYS_SINCE_LAST_ATTEMPT LOGIN_FAILURE_RATE LOGIN_SUCCESS_RATE".split()
non_numericals = "PREFERRED_CHANNEL PREFERRED_DEVICE LAST_LOGIN_STATUS LAST_DEVICE FIRST_LOGIN_STATUS FIRST_DEVICE".split()
#STANDARDIZE COLUMNS
active_customers[numericals] = active_customers[numericals].fillna(0).apply(lambda x: x.map(int))
active_customers[non_numericals] = active_customers[non_numericals].fillna("NA")

#GET TRANSACTIONS FOR THE LAST N MONTHS
last_n_months_txns = txns[txns.PERIOD<4].reset_index(drop=True)
txn_summary = last_n_months_txns.groupby("USERNAME")["TRANSACTION_STATUS"].value_counts().unstack().reset_index().fillna(0)
txn_summary["Failed Success".split()] = txn_summary["Failed Success".split()].apply(lambda x: x.map(int))
txn_summary["Total_Txns"] = txn_summary["Failed Success".split()].sum(axis=1)
txn_summary["Txn_Succcess_Rate"] = round((txn_summary["Success"]/txn_summary["Total_Txns"])*100,0).map(int)
txn_summary.columns =  [x.upper() for x in txn_summary.columns ]
txn_summary = txn_summary.merge(last_n_months_txns.groupby("USERNAME")["DATE"].max().reset_index().merge(last_n_months_txns["USERNAME DATE TRANSACTION_STATUS TYPE".split()],
on="USERNAME DATE".split()).rename({"DATE":"LAST_TXN_DATE","TRANSACTION_STATUS":"LAST_TXN_STATUS","TYPE":"LAST_TXN"},axis=1))
Quick_Banking_Summary = active_customers.merge(txn_summary,on="USERNAME",how="left")
txn_columns = "FAILED	SUCCESS	TOTAL_TXNS".split()
Quick_Banking_Summary[txn_columns] = Quick_Banking_Summary[txn_columns].fillna(0).apply(lambda x: x.map(int))
Quick_Banking_Summary["PERSONA"] = np.where((Quick_Banking_Summary.TOTAL_TXNS<1) & (Quick_Banking_Summary.LOGIN_ATTEMPTS<1),"Never logged in","")
Quick_Banking_Summary["PERSONA"] = np.where((Quick_Banking_Summary.TOTAL_TXNS<1)& (Quick_Banking_Summary.LOGIN_ATTEMPTS>0),"Logged in, Not Transacting",Quick_Banking_Summary.PERSONA)
Quick_Banking_Summary["PERSONA"] = np.where((Quick_Banking_Summary.TOTAL_TXNS>0) & (Quick_Banking_Summary.LOGIN_ATTEMPTS>0),"Logged in, Transacting",Quick_Banking_Summary.PERSONA)
Quick_Banking_Summary["PERSONA"] = np.where((Quick_Banking_Summary.TOTAL_TXNS>0) & (Quick_Banking_Summary.LOGIN_ATTEMPTS<1),"Not Logged in, Transacting",Quick_Banking_Summary.PERSONA)
Quick_Banking_Summary = Quick_Banking_Summary.drop_duplicates()
Quick_Banking_Summary["CONTACT"] = Quick_Banking_Summary["CONTACT"].str.replace("+","").str.replace("(","").str.replace(")","")
Quick_Banking_Summary["INACTIVE_PERIOD"] = np.where(Quick_Banking_Summary.LOGIN_ATTEMPTS>0, 9999999999,0)
Quick_Banking_Summary_1 =  Quick_Banking_Summary[Quick_Banking_Summary.INACTIVE_PERIOD==9999999999]; Quick_Banking_Summary_2 =  Quick_Banking_Summary[~(Quick_Banking_Summary.INACTIVE_PERIOD==9999999999)]
Quick_Banking_Summary_2["INACTIVE_PERIOD"] = Quick_Banking_Summary_2.ONBOARDED.apply(lambda x: (max_date - x)/np.timedelta64(1,'D'))
Quick_Banking_Summary = pd.concat([Quick_Banking_Summary_1,Quick_Banking_Summary_2]).drop_duplicates("CUSTOMER_ID").reset_index(drop=True)
Quick_Banking_Summary["INACTIVE_BRACKET"] =  Quick_Banking_Summary["INACTIVE_PERIOD"].apply(inactive_period)
print("|-- Almost done ...")
#REMOVE DUPLICATES
No_of_profiles = Quick_Banking_Summary.groupby("CUSTOMER_ID")["USERNAME"].nunique().reset_index().sort_values("USERNAME",ascending=False)
#Quick_Banking_Summary.groupby("CUSTOMER_ID")["USERNAME"]
No_of_transactions = Quick_Banking_Summary.groupby("CUSTOMER_ID")["TOTAL_TXNS"].sum().reset_index().merge(Quick_Banking_Summary.groupby("CUSTOMER_ID")["LAST_LOGIN_ATTEMPT"].max().reset_index().rename({"LAST_LOGIN_ATTEMPT":"HIGHEST_LOGIN"},axis=1),
on="CUSTOMER_ID").merge(No_of_profiles,on="CUSTOMER_ID").rename({"USERNAME":"NO_PROFILES","TOTAL_TXNS":"ALL_TXNS"},axis=1)
main = No_of_transactions.merge(Quick_Banking_Summary,left_on="CUSTOMER_ID HIGHEST_LOGIN".split(),
right_on="CUSTOMER_ID LAST_LOGIN_ATTEMPT".split(),how="left").sort_values("LAST_LOGIN_ATTEMPT",ascending=False).drop_duplicates(["CUSTOMER_ID"],keep="first").reset_index(drop=True)#.drop("BRANCH_Y",axis=1).rename({"BRANCH_X":"BRANCH"},axis=1)
main["CUSTOMER_STATUS"] = np.where(main["CUSTOMER_STATUS"].isna(),"Inactive",main["CUSTOMER_STATUS"])
drop_columns = "FIRST_DEVICE FIRST_LOGIN_STATUS FIRST_LOGIN_ATTEMPT STATUS".split()
new = main.drop(drop_columns,axis=1).rename({"STATUS_":"TRUSTFACTOR_STATUS"},axis=1)
new["DATE_OF_BIRTH"] = pd.to_datetime(new["DATE_OF_BIRTH"])
new=new.map(lambda x : str(x).replace('NaT',''))
year=pd.to_numeric(max_date.strftime('%Y'),errors='coerce')
new["AGE"]=new.DATE_OF_BIRTH.apply(lambda x : year - int(pd.to_datetime(x).strftime('%Y')) if len(x)>0  else x)
# txns['PERIOD'].map(str).apply(lambda x: str(x).replace('-','') if x.__contains__('-') else x)
# new["AGE"] = new.DATE_OF_BIRTH.apply(lambda x: (max_date - x)/np.timedelta64(1,'Y'))
new["AGE"] = pd.to_numeric(new["AGE"],errors='coerce')

new["CUSTOMER_TYPE"] = new["CUSTOMER_TYPE"].map(int).apply(lambda x: "Retail" if x == 0 else "Entity")
new["AGE_BRACKET"] = new["AGE"].apply(age_bracket)
final = new.merge(last_five_months_summary,on="USERNAME",how="left")
final["NEW_QB_USER"] =  np.where((final.LAST_90_DAYS_LAST_MONTH.isna()) & (final.LAST_90_DAYS_THIS_MONTH.notna()),1,0)
final.to_csv(files_path+"QB_Data.txt",index=False)
last_n_months_txns.to_csv(files_path+"QB_Transactions.txt",index=False)




# Transformed_contacted_customers = pd.read_sql(Transformed_contacted_customers_query,connection)
# Transformed_contacted_customers.to_csv(files_path+"Transformed_contacted_customers.txt",index=False)
# print("Transformed_contacted_customers")


Transformed_contacted_customers_latest = pd.read_sql(Transformed_contacted_customers_latest_query,connection)
Transformed_contacted_customers_latest.to_csv(files_path+"Transformed_contacted_customers_latest.txt",index=False)
print("Transformed_contacted_customers_latest")

customer_region_branch = pd.read_sql(customer_region_branch_query,connection)
customer_region_branch.to_csv(files_path+"customer_region_branch.txt",index=False)
print("Customers_branch_region_updated")


print("|-- Done!")
print("|-- "+"="*70)