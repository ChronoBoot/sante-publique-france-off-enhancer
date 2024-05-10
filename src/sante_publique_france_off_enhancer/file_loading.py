import pandas as pd


def load_csv():
    """Load the CSV file."""
    off_path = '../csv/fr.openfoodfacts.org.products.csv'
    off_df = pd.read_csv(off_path, delimiter='\t', low_memory=False)
    return off_df


def print_info(df: pd.DataFrame):
    """Print the number of products and the DataFrame info."""
    # Print the number of products
    print(f"Number of products {len(df)}")

    # Display the DataFrame info
    df.info()
