select idpaciente, data
				from (
					select max(data) data,idpaciente 
					from jsintomadiario 
					where valor is not null and data is not null and data != '0000-00-00'
					group by idpaciente 
				 ) idp 
				where data > '2019-12-31'
