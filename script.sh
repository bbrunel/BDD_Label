
while read p; do
	echo $p
	wget `echo $p | sed -E 's/"(.+)"/\1/'` -O - -o /dev/null | grep -oE "https://www.cnis.fr/wp-content[a-zA-Z0-9_/-]*(/){1}(AC|ac|ae|AE|AS|as|2-ac)[a-zA-Z0-9_-]+\.pdf" | tee -a results
done < BDD_Label/urls3.csv

