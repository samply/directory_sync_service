#cat data/crc-cohort-facts-full.csv | \
cat data/crc-cohort-facts-1patient.csv | \
# Remove column 8 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=8) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Correct ICD code
sed 's/urn:miriam:icd:urn:miriam:icd/urn:miriam:icd/' | \
# Remove arbitrary patient ID
sed 's/Cohort[^;]*;/Cohort;/' | \
# Remove quotes
sed 's/"//g' | \
sort | uniq > crc-cohort_facts.processed.csv

cat test/DirectoryFactTables.csv | \
# Remove column 10 (national_node), which is not in Petr's fact tables.
#awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=10) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove the left-over semicolon at the end of each line
sed 's/;$//' | \
# Remove column 8 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=8) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove arbitrary patient ID
sed 's/Cohort[^;]*;/Cohort;/' | \
sed 's/_collection_/:collection:/' | \
sort | uniq > DirectoryFactTables.processed.csv

if diff -u crc-cohort_facts.processed.csv DirectoryFactTables.processed.csv; then
    echo "Output matches against standard."
else
    echo "Output does not match against standard."
    exit 1
fi

