import csv

# Read the CSV file
input_file = './src/main/resources/nyc_taxi_sorted.csv'
output_file = './src/main/resources/nyc_taxi_medallion_sorted.csv'

print(f"Reading {input_file}...")

with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)  # Skip header
    rows = list(reader)

print(f"Read {len(rows)} rows")
print("Sorting by medallion (col 0) and pickup_datetime (col 5)...")

# Sort by column 0 (medallion) then column 5 (pickup_datetime)
sorted_rows = sorted(rows, key=lambda row: (row[0], row[5]))

print(f"Writing sorted data to {output_file}...")

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(sorted_rows)

print(f"Done! Sorted {len(sorted_rows)} rows.")
print("File saved as: nyc_taxi_medallion_sorted.csv")
