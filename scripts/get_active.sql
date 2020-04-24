select js.*, jsg.sintoma, jsg.tipo from 
	(select idsintomasGeral idsintoma, replace(Nome,";"," ") sintoma, tipo 
		from jsintomasgeral) jsg 
	join
	(select idsintoma, valor, data, jpf.* 
		from (select * from jsintomadiario where data is not null and data != '0000-00-00') jd 
		join 
		(select jp.idpaciente, Nome 
			from (select idpaciente 
				from (
					select max(data) data,idpaciente 
					from jsintomadiario 
					where valor is not null and data is not null and data != '0000-00-00'
					group by idpaciente 
				 ) idp 
				where data > '2019-12-31') idpf 
			join jpaciente jp 
			on jp.idpaciente = idpf.idpaciente) jpf 
		on jd.idpaciente = jpf.idpaciente
	) js		
	on jsg.idsintoma = js.idsintoma
