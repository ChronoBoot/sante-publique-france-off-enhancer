from pyspark.sql import Row
from pyspark.sql.functions import when, trim, lower, regexp_replace

from src.sante_publique_france_off_enhancer.classes.dataframe_field import DataFrameField
from src.sante_publique_france_off_enhancer.file_loading import *
from src.sante_publique_france_off_enhancer.classes.singleton_logger import SingletonLogger

logger = SingletonLogger.get_instance()
spark = SingletonSparkSession.get_instance()


def feature_listing(df: DataFrame) -> tuple[list[DataFrameField], list[DataFrameField], list[DataFrameField]]:
    logger.debug("Listing features")

    categorical_columns: list[DataFrameField] = []
    numerical_columns: list[DataFrameField] = []

    for name, dtype in df.dtypes:
        str_name = str(name)
        if dtype == 'int' or dtype == 'double':
            df_col = DataFrameField(name=str_name, dtype=dtype, field_type='numerical')
            numerical_columns.append(df_col)
        else:
            df_col = DataFrameField(name=str_name, dtype=dtype, field_type='categorical')
            categorical_columns.append(df_col)

    df_fields = categorical_columns + numerical_columns
    DataFrameField.process_and_set_na_percent(df_fields, df)

    logger.debug("Features listed")

    return categorical_columns, numerical_columns, df_fields


def dataframe_fields_to_dataframe(df_fields: list[DataFrameField]) -> DataFrame:
    logger.debug("Converting DataFrameFields to DataFrame")

    rows_data = []
    for field in df_fields:
        row_data = Row(
            name=field.name,
            dtype=field.dtype,
            na_percent=field.na_percent,
            fill_percent=field.fill_percent,
            field_type=field.field_type
        )
        rows_data.append(row_data)

    logger.debug("DataFrameFields converted to DataFrame")

    return spark.createDataFrame(rows_data)


def find_target(columns: list[DataFrameField]) -> list[DataFrameField]:
    logger.debug("Finding target")

    # noinspection PyShadowingNames
    candidates = [col for col in columns if col.na_percent > 50]

    logger.debug("Target found")

    return candidates


def get_recommended_target(columns: list[DataFrameField], nb_choice: int = 1) -> list[DataFrameField]:
    logger.debug("Getting recommended target")

    candidates = find_target(columns)
    targets = sorted(candidates, key=lambda candidate: candidate.na_percent)[:nb_choice]

    logger.debug("Recommended target found")

    return targets


def filter_fields(df: DataFrame, fields: list[str]) -> DataFrame:
    logger.debug("Filtering fields")
    return df[fields]


def format_identifier(df: DataFrame, identifier_name: str) -> DataFrame:
    logger.debug("Formatting identifier")

    df = df.withColumn(
        identifier_name,
        regexp_replace(lower(trim(col(identifier_name))), " ", "")
    )

    logger.debug("Identifier formatted")

    return df


def format_col(df: DataFrame, col_name: str, unknown_value: str = 'unknown') -> DataFrame:
    logger.debug(f"Formatting {col_name}")

    cleaned_col = lower(trim(col(col_name)))

    df = df.withColumn(
        col_name,
        when(
            (cleaned_col.isNull()) | (cleaned_col == unknown_value),
            None
        ).otherwise(cleaned_col)
    )

    logger.debug(f"{col_name} formatted")

    return df


def drop_duplicates(df: DataFrame, identifier_name: str) -> DataFrame:
    logger.debug("Dropping duplicates")
    return df.drop_duplicates([identifier_name])


def delete_duplicates(df: DataFrame, identifier_name: str) -> DataFrame:
    logger.debug("Deleting duplicates")

    formatted_df = format_identifier(df, identifier_name)

    formatted_df = drop_duplicates(formatted_df, identifier_name)

    return formatted_df


def filtered_row(df: DataFrame, targets_names: list[str], incorrect_value=None):
    logger.debug("Filtering rows")

    filtered_off_df = df.dropna(subset=targets_names)

    if incorrect_value is not None:
        for target_name in targets_names:
            filtered_off_df = filtered_off_df.filter(df[target_name] != incorrect_value)

    logger.debug("Rows filtered")

    return filtered_off_df


@cache
def cleaning_and_filtering_of_features_and_products(df: DataFrame) -> tuple[DataFrame, list[DataFrameField]]:
    logger.info(f"Before cleaning and filtering: {df.count()} products.")

    categorical_columns, numerical_columns, df_fields = feature_listing(df)

    targets_names = ["pnns_groups_1", "pnns_groups_2"]

    filtered_off_df = format_col(df, "pnns_groups_1")
    filtered_off_df = format_col(filtered_off_df, "pnns_groups_2")

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

    logger.info(f"After cleaning and filtering: {filtered_off_no_duplicates.count()} products.")

    return filtered_off_no_duplicates, filtered_df_fields


if __name__ == "__main__":
    # Example usage
    off_df = load_csv()
    cleaned_data, cleaned_fields = cleaning_and_filtering_of_features_and_products(off_df)
