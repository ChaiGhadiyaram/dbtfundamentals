--test
select  patient_pathway_id, 
 		case when 
		replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'lastName' is not null
		then replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'lastName'
		 else null end as lastName,
	 	case when 
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'firstName' is not null
			then replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'firstName'
		 else null end as firstName,
		 case when  
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'middleName' is not null
			then replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'middleName'
		 else null end as middleName,
  		 case when 
		 	replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'dateOfBirth' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'dateOfBirth'
		 else null end as dateOfBirth,
    	case when 
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'clientId' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'clientId'
			when
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'client_id' is not null
			then
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'client_id'  
		 else null end as clientId,
    	case when 
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'clientName' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'clientName'
		 else null end as clientName,
      	case when 
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'groupNumber' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'groupNumber'
  			when replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'group_number' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'group_number' 
		 else null end as groupNumber,
        case when 
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'groupName' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'groupName'
		 else null end as groupName,
        case when 
			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'businessSegmentGroupName' is not null
			then
  			replace(replace(client_info,'{"eligibility":[',''),']}','')::json->'businessSegmentGroupName'
		 else null end as businessSegmentGroupName
  from {{ ref('t_cigna_s3_monthly_drop_intg') }}
  where -- patient_pathway_id in (44763,44761,44739,30209) and 
  ((client_info  like '%]}' and split_part(client_info,':',1) not like '%client_id%')
   or (client_info  like '%}' and 
	  (split_part(client_info,':',1) like '%client_id%' or split_part(client_info,':',1) like '%group_number%')))
  and length(client_info::text) < 400 