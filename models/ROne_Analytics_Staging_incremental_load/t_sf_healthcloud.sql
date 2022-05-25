select sf_id, 
"Name" Name,
 "HealthCloudGA__BirthDate__pc" HealthCloudGA__BirthDate__pc,
  "PersonEmail" PersonEmail,
   "Individual_Enterprise_Id__c" Individual_Enterprise_Id__c,
    "Client_Id__c" Client_Id__c,
     "Assigned_to_HMS__c" Assigned_to_HMS__c,
      "Do_Not_IVR__pc" Do_Not_IVR__pc, 
      "Do_Not_SMS__pc" Do_Not_SMS__pc, 
      "Do_Not_Email_optout__pc" Do_Not_Email_optout__pc, 
      "Do_Not_Mail__pc" Do_Not_Mail__pc,
       "Employer_Level_Opt_Out__c" Employer_Level_Opt_Out__c, 
       "Marketing_Enrollment_Complete__c" Marketing_Enrollment_Complete__c, 
       "Present_on_This_Month_s_Eligibility_File__c" Present_on_This_Month_s_Eligibility_File__c, 
       "Sync_to_Marketing_Cloud__pc" Sync_to_Marketing_Cloud__pc
	
from ro_data.t_sf_healthcloud_raw