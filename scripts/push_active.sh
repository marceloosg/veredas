mysql_pass=$(cat mysql_pass)
mysql_user=$(cat mysql_user)
mysql_db=$(cat mysql_db)
mysql_host=$(cat mysql_host)


mysql -u$mysql_user -p$mysql_pass $mysql_db -h $mysql_host --default-character-set="latin1" < get_active.sql  > active.txt
cat active.txt |sed "s/\t/;/g" > active.csv
