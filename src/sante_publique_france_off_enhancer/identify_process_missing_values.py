"""## Step 3 : Identify and process missing values"""
import os.path

from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum

from sante_publique_france_off_enhancer.classes.nutrition_facts import NutritionFacts
from sante_publique_france_off_enhancer.classes.singleton_logger import SingletonLogger
from sante_publique_france_off_enhancer.cleaning_filtering_features_products import \
    cleaning_and_filtering_of_features_and_products
from src.sante_publique_france_off_enhancer.classes.na_count import NaCount
from src.sante_publique_france_off_enhancer.file_loading import load_csv
from src.sante_publique_france_off_enhancer.classes.singleton_spark_session import SingletonSparkSession
from src.sante_publique_france_off_enhancer.identify_process_abnormal_values import abnormal_values_processing, \
    CSV_FILE_PATH as ABNORMAL_VALUES_PROCESSED_CSV_FILE_PATH

CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')
CSV_FILE_NAME = "off_cleaned_and_filtered.csv"
CSV_FILE_PATH = os.path.join(CSV_DIR, CSV_FILE_NAME)

logger = SingletonLogger.get_instance()
spark = SingletonSparkSession.get_instance()


def get_na_counts(df: DataFrame) -> DataFrame:
    fields = df.columns

    na_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in fields])

    return na_counts


def nutrition_score_to_nutrition_grade_impl(nutrition_score: float, is_beverage: bool) -> str:
    if is_beverage:
        # Explain why I did not implement that
        raise NotImplementedError("Beverage conversion is not handled")
    else:
        if nutrition_score <= -1:
            return 'a'
        elif 0 <= nutrition_score <= 2:
            return 'b'
        elif 3 <= nutrition_score <= 10:
            return 'c'
        elif 11 <= nutrition_score <= 18:
            return 'd'
        else:  # 19 and above
            return 'e'


def nutrition_score_to_nutrition_grade(nutrition_score: float, pnns_main_group: str) -> str:
    return nutrition_score_to_nutrition_grade_impl(nutrition_score, pnns_main_group.lower() == 'beverages')


def is_beverage_category(df: DataFrame, row_index: int) -> bool:
    row = df.collect()[row_index]
    return row['pnns_groups_1'].lower() == 'beverages'


def grade_letter_to_number(letter: str) -> int:
    if letter == 'a':
        return 0
    elif letter == 'b':
        return 1
    elif letter == 'c':
        return 2
    elif letter == 'd':
        return 3
    elif letter == 'e':
        return 4
    else:
        raise ValueError(f"Unknown grade value: {letter}")



# def fill_additives(row: pd.Series) -> pd.Series:
#     if row['additives_n'] is None:
#         row['additives_n'] = row['median_additives_n']
#     if row['ingredients_from_palm_oil_n'] is None:
#         row['ingredients_from_palm_oil_n'] = row['median_ingredients_from_palm_oil_n']
#     if row['ingredients_that_may_be_from_palm_oil_n'] is None:
#         row['ingredients_that_may_be_from_palm_oil_n'] = row['median_ingredients_that_may_be_from_palm_oil_n']
#
#     return row


def set_median_values(df: DataFrame) -> DataFrame:
    columns = [
        'additives_n',
        'ingredients_from_palm_oil_n',
        'ingredients_that_may_be_from_palm_oil_n',
        'energy_100g',
        'fat_100g',
        'saturated-fat_100g',
        'carbohydrates_100g',
        'sugars_100g',
        'fiber_100g',
        'proteins_100g',
        'salt_100g',
        'sodium_100g',
        'nutrition-score-fr_100g',
        'fruits-vegetables-nuts_100g'
    ]

    for column in columns:
        df = add_median_value(df, column)

    return df


def add_median_value(df: DataFrame, column_name: str) -> DataFrame:
    pnns_groups_2_values = df['pnns_groups_2'].unique()

    for pnns_group_2 in pnns_groups_2_values:
        median_value = get_median_value(df, column_name, pnns_group_2)
        df.loc[df['pnns_groups_2'] == pnns_group_2, f"median_{column_name}"] = median_value

    return df


def get_median_value(df: DataFrame, column_name: str, pnns_group_2: str) -> float:
    return df.loc[df['pnns_groups_2'] == pnns_group_2, column_name].median()


# def fill_missing_values_with_pnns_groups_2_median(row: pd.Series) -> pd.Series:
#     columns = [
#         'additives_n',
#         'ingredients_from_palm_oil_n',
#         'ingredients_that_may_be_from_palm_oil_n',
#         'energy_100g',
#         'fat_100g',
#         'saturated-fat_100g',
#         'carbohydrates_100g',
#         'sugars_100g',
#         'fiber_100g',
#         'proteins_100g',
#         'salt_100g',
#         'sodium_100g',
#         'fruits-vegetables-nuts_100g'
#     ]
#
#     for column in columns:
#         if row[column] is None:
#             row[column] = row[f"median_{column}"]
#
#     return row


if __name__ == '__main__':
    if os.path.exists(ABNORMAL_VALUES_PROCESSED_CSV_FILE_PATH):
        cleaned_data_with_abnormal_values_processed = load_csv(ABNORMAL_VALUES_PROCESSED_CSV_FILE_PATH)
    else:
        off_df = load_csv()
        cleaned_data_with_abnormal_values_processed = (
            abnormal_values_processing(cleaning_and_filtering_of_features_and_products(off_df)[0]))

    logger.info(f"Number of nutriscore missing before processing: "
                f"{NutritionFacts.get_nb_nutriscore_missing(cleaned_data_with_abnormal_values_processed)}")

    cleaned_data_with_abnormal_and_missing_values_processed = (
        NutritionFacts.calculate_nutriscore(cleaned_data_with_abnormal_values_processed))

    logger.info(f"Number of nutriscore missing after processing: "
                f"{NutritionFacts.get_nb_nutriscore_missing(cleaned_data_with_abnormal_and_missing_values_processed)}")

