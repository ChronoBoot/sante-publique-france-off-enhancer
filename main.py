import pandas as pd
import os

# Load the data
# Get the directory of the script
script_dir = os.path.dirname(os.path.abspath(__file__))
off_csv_name = 'fr.openfoodfacts.org.products.csv'
csv_directory = 'csv'
off_csv_path = os.path.join(script_dir, csv_directory, off_csv_name)

off_df = pd.read_csv(off_csv_path, sep='\t')
print(off_df.head())
