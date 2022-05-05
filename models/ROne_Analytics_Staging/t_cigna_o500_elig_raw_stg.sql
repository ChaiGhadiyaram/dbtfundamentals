
select * from (
select dense_rank() over(partition by substr(ami,1,9),email_addr_txt order by ami,
						 	subscrbr_stat_cd,cust_elgbty_cvrg_term_dt desc  ) as rn,*
from {{ref('t_cigna_o500_elig_raw_intg')}} where email_addr_txt is not null --and ami like '000591356%'
union
select dense_rank() over(partition by substr(ami,1,9),mobile_pn order by ami,
						 	subscrbr_stat_cd,cust_elgbty_cvrg_term_dt desc  ) as rn,*
from {{ref('t_cigna_o500_elig_raw_intg')}} where mobile_pn is not null 
union
select dense_rank() over(partition by substr(ami,1,9),cust_addr_ln_1 order by ami,
						 	subscrbr_stat_cd,cust_elgbty_cvrg_term_dt desc  ) as rn,*
from {{ref('t_cigna_o500_elig_raw_intg')}} where cust_addr_ln_1 is not null 
) as a where a.rn=1 