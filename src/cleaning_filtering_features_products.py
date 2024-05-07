from src.classes.dataframe_field import DataFrameField
from src.file_loading import *


def feature_listing(df: pd.DataFrame) -> tuple[list[DataFrameField], list[DataFrameField], list[DataFrameField]]:
    categorical_columns = []
    numerical_columns = []

    for name, dtype in df.dtypes.items():
        if dtype == 'int64' or dtype == 'float64':
            df_col = DataFrameField(name=str(name), dtype=dtype, field_type='numerical')
            df_col.process_and_set_na_percent(df[name])
            numerical_columns.append(df_col)
        else:
            df_col = DataFrameField(name=str(name), dtype=dtype, field_type='categorical')
            df_col.process_and_set_na_percent(df[name])
            categorical_columns.append(df_col)

    df_fields = categorical_columns + numerical_columns

    return categorical_columns, numerical_columns, df_fields


def dataframe_fields_to_dataframe(df_fields: list[DataFrameField]) -> pd.DataFrame:
    rows_data = []
    for field in df_fields:
        row_data = {
            'name': field.name,
            'dtype': field.dtype,
            'na_percent': field.na_percent,
            'fill_percent': field.fill_percent,
            'field_type': field.field_type
        }
        rows_data.append(row_data)

    return pd.DataFrame(rows_data)


def find_target(columns: list[DataFrameField]) -> list[DataFrameField]:
    candidates = [col for col in columns if col.na_percent > 50]
    return candidates


def get_recommended_target(columns: list[DataFrameField], nb_choice: int = 1) -> list[DataFrameField]:
    candidates = find_target(columns)
    targets = sorted(candidates, key=lambda candidate: candidate.na_percent)[:nb_choice]
    return targets


def filter_fields(df: pd.DataFrame, fields: list[str]) -> pd.DataFrame:
    return df[fields]


def format_identifier(df: pd.DataFrame, identifier_name: str) -> pd.DataFrame:
    df[identifier_name] = df[identifier_name].str.lower().str.strip().replace(" ", "")
    return df


def format_pnns_groups_1(row: pd.Series) -> pd.Series:
    if not isinstance(row['pnns_groups_1'], str) or row['pnns_groups_1'] == 'unknown':
        row['pnns_groups_1'] = 'None'

    row['pnns_groups_1'] = row['pnns_groups_1'].lower().strip().replace(" ", "")

    return row


def format_pnns_groups_2(row: pd.Series) -> pd.Series:
    if not isinstance(row['pnns_groups_2'], str) or row['pnns_groups_2'] == 'unknown':
        row['pnns_groups_2'] = 'None'

    row['pnns_groups_2'] = row['pnns_groups_2'].lower().strip().replace(" ", "")

    return row


def drop_duplicates(df: pd.DataFrame, identifier_name: str) -> pd.DataFrame:
    return df.drop_duplicates(subset=identifier_name)


def delete_duplicates(df: pd.DataFrame, identifier_name: str) -> pd.DataFrame:
    formatted_df = format_identifier(df, identifier_name)
    return drop_duplicates(formatted_df, identifier_name)


def filtered_row(df, targets_names, incorrect_value=None):
    filtered_off_df = df.dropna(subset=targets_names)

    if incorrect_value is not None:
        for target_name in targets_names:
            filtered_off_df = filtered_off_df.loc[df[target_name] != incorrect_value]

    return filtered_off_df


def cleaning_and_filtering_of_features_and_products(df: pd.DataFrame) -> tuple[pd.DataFrame, list[DataFrameField]]:
    categorical_columns, numerical_columns, df_fields = feature_listing(df)

    targets_names = ["pnns_groups_1", "pnns_groups_2"]

    filtered_off_df = df.apply(format_pnns_groups_1, axis=1)
    filtered_off_df = filtered_off_df.apply(format_pnns_groups_2, axis=1)

    filtered_off_df = filtered_row(filtered_off_df, targets_names)

    features_names = [
        "additives_n", "ingredients_from_palm_oil_n", "ingredients_that_may_be_from_palm_oil_n",
        "energy_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g",
        "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g", "nutrition-score-fr_100g",
        "nutrition_grade_fr", "fruits-vegetables-nuts_100g"
    ]

    filtered_df_fields = [field for field in df_fields if field.name in features_names]
    identifier_name = 'product_name'

    all_fields = features_names + targets_names + [identifier_name]
    filtered_off_df = filter_fields(filtered_off_df, all_fields)

    filtered_off_no_duplicates = delete_duplicates(filtered_off_df, identifier_name)

    return filtered_off_no_duplicates, filtered_df_fields


if __name__ == "__main__":
    # Example usage
    off_df = load_csv()
    print(f"Before cleaning and filtering: {len(off_df)} products.")
    cleaned_data, cleaned_fields = cleaning_and_filtering_of_features_and_products(off_df)
    print(f"After cleaning and filtering: {len(cleaned_data)} products.")
