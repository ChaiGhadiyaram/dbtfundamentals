

SELECT "Client Name (Official)" as Client_Name_Official, 
"Client Name (DBA)" as Client_Name_DBA, 
"Client ID Number" as Client_ID_Number, 
"Account Number(s)" as Account_Numbers, 
"ROne Practice ID" ROne_Practice_ID, 
"Eligible Lives" Eligible_Lives, 
"Cigna Client AM/AE" Cigna_client_AM_AE, 
"NA/O500" NA_O500, 
"CS Owner" CS_Owner, 
"License to Receive Elig and Trigger Files" License_to_receive_Elig_and_trigger_file, 
"Go Live Date (Phase 2)" Go_Live_date_Phase_2, 
"Client Active or Termed" Client_Active_or_termed, 
"Special Cases
 (Own Track of Comms)" Special_cases_own_track_of_comms, 
"Marketing Launch Date" marketing_launch_date, 
"Email" email, 
"SMS" sms, 
"Direct Mail" direct_mail, 
"Custom Disclaimer?" custom_disclaimer, 
"Employee Naming Convention (associates, members, employees, etc" employee_naming_convention, 
"Logo Provided to MKT?" logo_provided_by_mkt, 
"Date Last Modified" date_last_modified, 
"Comments" comments, 
"Historical Comms Comments" historical_comms_comments
	FROM ro_data."t_gsheet_CSM_Owned_Marketing_OptOut_raw"