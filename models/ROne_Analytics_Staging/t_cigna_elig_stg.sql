{{ config(
    pre_hook=

"update ro_data.t_cigna_elig_intg tgt set email_addr_txt = null 
from (
select * from (
select stg.*,row_number() over(partition by stg.email_addr_txt order by ami) as rn
from ro_data.t_cigna_elig_intg stg ,
	(select distinct email_addr_txt from (select email_addr_txt, count(*)
				from ro_data.t_cigna_elig_intg 
				group by 1 having count(*) > 1) a) email 
	
	where email.email_addr_txt = stg.email_addr_txt-- and pid = '475480504959527' 
			) as b where rn > 1) src
where tgt.ami = src.ami  and tgt.email_addr_txt = src.email_addr_txt
and tgt.pid = src.pid and tgt.dependent_code = src.dependent_code"
) }}

{{ config(
    pre_hook=

"update ro_data.t_cigna_elig_intg tgt set mobile_pn = null 
from (
select * from (
select stg.*,row_number() over(partition by stg.mobile_pn order by ami) as rn
from ro_data.t_cigna_elig_intg stg ,
	(select distinct mobile_pn from (select mobile_pn, count(*)
				from ro_data.t_cigna_elig_intg 
				group by 1 having count(*) > 1) a) email 
	
	where email.mobile_pn = stg.mobile_pn-- and pid = '475480504959527' 
			) as b where rn > 1) src
where tgt.ami = src.ami  and tgt.mobile_pn = src.mobile_pn
and tgt.pid = src.pid and tgt.dependent_code = src.dependent_code"
) }}

select 
ami, dependent_code, pid, indiv_entpr_id,  client_id ,
client_acct_num, client_acct_nm, initcap(cust_frst_nm) cust_frst_nm, 
initcap(cust_last_nm) cust_last_nm, mobile_pn, email_addr_txt, cust_addr_ln_1, cust_addr_ln_2, cust_city_nm, cust_st_cd, 
cust_postl_cd, vrbl_lang_prefnc_cd, prodt_ty_cd, cust_elgbty_cvrg_eff_dt, cust_elgbty_cvrg_term_dt, 
subscrbr_stat_cd, cust_brth_dt, cust_gendr_cd, cvrg_per_yr_mth_num, 
	case when cast(client_id as int) in (17319, 62491, 66562) then 'O500' 
		when file_name like '%u500%' and cast(client_id as int) in 
		(46187	,51515	,47595	,6933	,53572	,41705	,53595	,14438	,56295	,62336	,23974	,55194	,42222	,
41670	,6938	,45739	,59230	,60570	,60570	,51309	,38332	,38332	,22986	,12682	,58952	,7492	,
39676	,12950	,58332	,65031	,53293	,34022	,9196	,53589	,13589	,40186	,16401	,58869	,15422	,
48000	,62346	,14326	,56903	,37955	,40425	,66562	,14677	,37259	,41497	,35014	,62281	,64512	,
54020	,62491	,10067	,45126	,40979	,62021	,17319	,37296	,46205	,65264	,34605	,22986	,45682	,
53619	,45001	,23222	,14558	,43582	,43412	,56289	,47786	) then 'O500'
		when COALESCE(client_id::char,'DEF') = 'DEF' then 'O500' end as segment
, funding, sic, file_name


from (
select dense_rank() over(partition by substr(ami,1,9),email_addr_txt order by ami,
subscrbr_stat_cd,cust_elgbty_cvrg_term_dt desc  ) as rn,*
from {{ ref('t_cigna_elig_intg') }} where email_addr_txt is not null --and ami like '000591356%'
union
select dense_rank() over(partition by substr(ami,1,9),mobile_pn order by ami,
subscrbr_stat_cd,cust_elgbty_cvrg_term_dt desc  ) as rn,*
 from {{ ref('t_cigna_elig_intg') }} where mobile_pn is not null
) as a where a.rn=1 

{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column med_claim_triggered varchar") }}
{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column med_claim_trig_dt varchar") }}
{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column med_claim_trig_days varchar") }}
{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column email_opt_out varchar") }}
{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column phone_opt_out varchar") }}
{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column mailing_opt_out varchar") }}
{{ config( post_hook=" alter table ro_data.t_cigna_elig_stg add column c;ient_opt_out varchar") }}