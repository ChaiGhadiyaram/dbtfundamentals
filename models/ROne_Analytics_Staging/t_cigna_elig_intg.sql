select * from ro_data.t_cigna_o500_elig_raw
where file_name in (select  max(file_name) from ro_data.t_cigna_o500_elig_raw)
				   union
select * from ro_data.t_cigna_u500_elig_raw
where file_name in (select  max(file_name) from ro_data.t_cigna_u500_elig_raw)