import csv
from faker import Faker
from multiprocessing import Pool, cpu_count
import os

def generate_fake_data(batch_size):
    fake = Faker()
    data = []
    for _ in range(batch_size):
        data.append([
            fake.first_name(),
            fake.last_name(),
            fake.address().replace("\n", ", "),
            fake.date_of_birth(minimum_age=18, maximum_age=90)
        ])
    return data

def write_csv(file_name, rows=3*10**7, batch_size=10000):
    total_batches = rows // batch_size
    remaining_rows = rows % batch_size

    # Write headers
    with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["first_name", "last_name", "address", "date_of_birth"])

    # Use multiprocessing to generate and write data
    with Pool(cpu_count()) as pool:
        for batch_data in pool.imap_unordered(generate_fake_data, [batch_size] * total_batches):
            with open(file_name, 'a', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(batch_data)

    # Write remaining rows
    if remaining_rows > 0:
        remaining_data = generate_fake_data(remaining_rows)
        with open(file_name, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(remaining_data)

# Generate the CSV
if __name__ == "__main__":
    file_name = "large_dataset.csv"
    rows = 3*10**7  # 30 million rows
    batch_size = 10000
    write_csv(file_name, rows, batch_size)
