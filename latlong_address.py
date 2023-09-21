import requests
import pandas as pd
import os

# split csv into many files for easier processing
import csv

# Splitting CSV 
input_file = 'osopenuprn_202308.csv' # load in august release
output_folder = 'raw_output_uprn_split'  # Change this to your desired folder name
rows_per_file = 1000

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def find_last_output_file_number(output_folder):
    files = os.listdir(output_folder)
    numbers = []
    for file in files:
        if file.startswith('output_') and file.endswith('.csv'):
            try:
                number = int(file.split('_')[1].split('.')[0])
                numbers.append(number)
            except ValueError:
                continue
    if numbers:
        return max(numbers)
    else:
        return 0

# Find the last created output file number (in case splitting fails)
last_output_file_number = find_last_output_file_number(output_folder)

# Corrected file handling structure
with open(input_file, 'r') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Read the header
    file_number = last_output_file_number + 1  # Start from the next number
    current_rows = 0
    # Create the output file with the next available number
    output_file = os.path.join(output_folder, f'output_{file_number}.csv')
    output_csv = open(output_file, 'w', newline='')
    writer = csv.writer(output_csv)
    writer.writerow(header)  # Write the header to each output file
    for row in reader:
        writer.writerow(row)
        current_rows += 1
        if current_rows >= rows_per_file:
            output_csv.close()  # Close the output file
            file_number += 1
            current_rows = 0
            output_file = os.path.join(output_folder, f'output_{file_number}.csv')
            output_csv = open(output_file, 'w', newline='')
            writer = csv.writer(output_csv)
            writer.writerow(header)


# version 5, up-to-date 
# Function to perform reverse geocoding with error handling and retries
import time
def reverse_geocode_with_retry(lat, lon, max_retries=3):
    retry_count = 0
    while retry_count < max_retries:
        try:
            url = f"https://nominatim.openstreetmap.org/reverse.php?format=jsonv2&lat={lat}&lon={lon}&zoom=18"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'display_name' in data:
                    full_address = data['display_name']
                    return full_address
                elif 'address' in data:
                    address = data['address']
                    postcode = address.get('postcode', '')
                    road = address.get('road', '')
                    city = address.get('city', '')
                    state = address.get('state', '')
                    country = address.get('country', '')
                    full_address = f"{road}, {city}, {state}, {postcode}, {country}"
                    return full_address
            # If there's a rate limit error, wait for a while and then retry
            elif response.status_code == 429:
                print(f"Rate limit exceeded. Waiting and retrying (attempt {retry_count + 1}/{max_retries})...")
                time.sleep(60)  # Wait for 60 seconds before retrying
            # Handle other HTTP errors
            else:
                print(f"HTTP error {response.status_code}. Retrying (attempt {retry_count + 1}/{max_retries})...")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying (attempt {retry_count + 1}/{max_retries})...")
        retry_count += 1
    print(f"Failed to retrieve address after {max_retries} attempts. Returning None.")
    return None

# Directory containing the CSV files
csv_directory = "raw_output_uprn_split"  # Replace with the path to your directory
output_csv_file = "combined_addresses.csv"
failures_csv_file = "geocoding_failures.csv"  # File to store failed records

if not os.path.exists(output_csv_file):
    header_written = False
else:
    header_written = True

# Create an empty DataFrame to store failed records
failures_df = pd.DataFrame(columns=["LATITUDE", "LONGITUDE"])

# List of specific CSV file numbers to process (e.g., [1, 3, 5])
csv_files_to_process = [1:]  # Replace with the file numbers you want to process

# Iterate through the chosen CSV files
for file_number in csv_files_to_process:
    input_csv_file = os.path.join(csv_directory, f"output_{file_number}.csv")
    # Load the chosen CSV file
    df = pd.read_csv(input_csv_file)
    # Initialize a column for the addresses
    df['Address'] = ""
    # Iterate through rows and reverse geocode
    for index, row in df.iterrows():
        address = reverse_geocode_with_retry(row['LATITUDE'], row['LONGITUDE'])
        if address is not None:
            df.at[index, 'Address'] = address
        else:
            # Record the failure in the failures DataFrame
            failures_df = failures_df.append({"LATITUDE": row['LATITUDE'], "LONGITUDE": row['LONGITUDE']}, ignore_index=True)
    # Append to the output file, but only write the header for the first file
    if not header_written:
        df.to_csv(output_csv_file, mode='w', index=False)
        header_written = True
    else:
        df.to_csv(output_csv_file, mode='a', header=False, index=False)

# Save the failures to a separate CSV file
if not failures_df.empty:
    failures_df.to_csv(failures_csv_file, index=False)
    print(f"Failures saved to {failures_csv_file}")

print(f"Addresses written to {output_csv_file}")
