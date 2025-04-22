# Preprocess standard fact table data
cat data/crc-cohort-facts-full.csv | \
# Remove column 8 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=8) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Correct ICD code
sed 's/urn:miriam:icd:urn:miriam:icd/urn:miriam:icd/' | \
# Remove arbitrary patient ID
sed 's/Cohort[^;]*;/Cohort;/' | \
# Remove quotes
#sed 's/"//g' | \
# Remove header row
grep -v "sample_type;number_of_donors;number_of_samples" | \
sort > crc-cohort_facts.processed.csv

# Preprocess generated output fact table data
cat test/DirectoryFactTables.csv | \
# Remove column 10 (national_node), which is not in Petr's fact tables.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=10) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove the left-over semicolon at the end of each line
sed 's/;$//' | \
# Remove column 8 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=8) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove arbitrary patient ID
sed 's/Cohort[^;]*;/Cohort;/' | \
sed 's/_collection_/:collection:/' | \
# Remove header row
grep -v "sample_type;number_of_donors;number_of_samples" | \
sort > DirectoryFactTables.processed.csv

# Compare fact tables
if diff crc-cohort_facts.processed.csv DirectoryFactTables.processed.csv; then
    echo "Fact table output matches against standard."
else
    echo "Fact table output does not match against standard."
    exit 1
fi

# Preprocess standard fact table data
cat data/crc-cohort-collections-full.csv | \
# Remove column 7 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=7) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove header row
grep -v "order_of_magnitude" | \
sort > crc-cohort_collections.processed.csv

# Preprocess generated output fact table data
cat test/DirectoryCollections.csv | \
# Remove column 7 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=7) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove header row
grep -v "order_of_magnitude" | \
sort > DirectoryCollections.processed.csv

# Compare collections
if diff crc-cohort_collections.processed.csv DirectoryCollections.processed.csv; then
    echo "Collections output matches against standard."
else
    echo "Collections output does not match against standard."
    exit 1
fi

