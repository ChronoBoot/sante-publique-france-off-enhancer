import pandas as pd
import os
from data_frame_field import DataFrameField

def load_csv():
    # Load the data
    # Get the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    off_csv_name = 'fr.openfoodfacts.org.products.csv'
    csv_directory = 'csv'
    off_csv_path = os.path.join(script_dir, csv_directory, off_csv_name)
    
    off_df = pd.read_csv(off_csv_path, sep='\t')
    return off_df

"""## Step 1 : Cleaning and filtering of features and products

### Feature listing

Split the columns attributes in 2, categorical and numerical
"""

def feature_listing(off_df):
  categorical_columns = []
  numerical_columns = []

  for name, dtype in off_df.dtypes.items():
    na_percent = DataFrameField.process_na_percent(off_df[name])

    if(dtype == 'int64' or dtype == 'float64'):
      df_col = DataFrameField(name, dtype, na_percent, field_type='numerical')
      numerical_columns.append(df_col)
    else:
      df_col = DataFrameField(name, dtype, na_percent, field_type='categorical')
      categorical_columns.append(df_col)

  df_fields = categorical_columns + numerical_columns

  return categorical_columns, numerical_columns, df_fields


"""### Find a target

We find which columns have more than 50% of values missing and we return the one with the most amount of non-missing values.
We will prefer to have the target to be as close a 50% missing value as possible because with too little values, prediction is going to be harder and less reliable
"""

def find_target(columns):
  candidates = []
  for col in columns :
    if col.na_percent > 50 :
      candidates.append(col)

  return candidates

def get_recommanded_target(columns, nb_choice = 1):
  candidates = find_target(columns)
  targets = []

  for i in range (0, nb_choice):
    target = min(candidates, key = lambda candidate : candidate.na_percent)
    targets.append(target)
    candidates.remove(target)

  return targets

"""The chosen target is pnns_group_1 which most like stands for PNNS (Programme National Nutrition Santé) which categorize foods.

### Remove rows without the target value
"""

def filtered_row(df, target_name):
  filtered_off_df = df[df[target_name].notna()]
  return filtered_off_df

"""### Separate target from dataset"""

def separate_target_from_dataset(target_name, df, df_fields):
  target_col = df[target_name]
  filtered_off_df = df.drop(target_name, axis=1)
  df_fields = [field for field in df_fields if field.name != target_name]

  return target_col, filtered_off_df, df_fields

"""### Display the fill rates of the features of the dataset"""

def display_rate(df_fields):
  for field in df_fields :
    field.process_and_set_na_percent(filtered_off_df[field.name])
    print(f"Fill rate {field.name} : {field.fill_percent}")

"""### Feature selection

We select features with more than 50% of non-missing values which could be use for predicting our target
"""

def features_selection(df_fields):
  features = []

  for field in df_fields:
    if(field.fill_percent > 50):
      features.append(field)

  return features

"""The features that have been selected are as follow :

- ingredients_text
- additives
- additives_n
- ingredients_from_palm_oil_n
- ingredients_that_may_be_from_palm_oil_n
- energy_100g
- fat_100g
- saturated-fat_100g
- carbohydrates_100g
- sugars_100g
- fiber_100g
- proteins_100g
- salt_100g
- sodium_100g

They are focus on the ingredients or nutrition facts

### Delete duplicates

We consider that 2 products with the same name are a duplicate
"""

def delete_duplicates(df, identifier_name):
   df[identifier_name] = df[identifier_name].str.lower().str.strip().str.replace(" ", "")
   no_duplicate_df = df.drop_duplicates(subset=identifier_name)
   return no_duplicate_df

"""### Main function to clean and filter features and products"""

def main(df):
  categorical_columns, numerical_columns, df_fields = feature_listing(df)

  candidates = find_target(categorical_columns)

  target_name = "pnns_groups_1"

  filtered_off_df = filtered_row(df, target_name)
  print(f"Number of lines with {target_name} : {len(filtered_off_df)}")

  target_col, filtered_off_df, df_fields = separate_target_from_dataset(target_name, filtered_off_df, df_fields)

  #display_rate(df_fields)

  features = features_selection(df_fields)

  identifier_name = 'product_name'

  filtered_off_no_duplicates = delete_duplicates(filtered_off_df, identifier_name)
  print(f"Number of lines with {target_name} and without duplicates: {len(filtered_off_no_duplicates)}")

if __name__ == "__main__":
    main(load_csv())

"""## Step 2 : Identify and process incoherent values

## Step 3 : Identify and process missing values

## Step 4 : Perform univariate and bivariate analyses

## Step 5 : Perform a multivariate analysis
"""