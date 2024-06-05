"""## Step 2 : Identify and process abnormal values"""
import os
from functools import cache

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import seaborn as sns
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, when, col
from pyspark.sql.types import Row

from sante_publique_france_off_enhancer.classes.singleton_logger import SingletonLogger
from sante_publique_france_off_enhancer.classes.singleton_spark_session import SingletonSparkSession
from sante_publique_france_off_enhancer.cleaning_filtering_features_products import \
    CSV_FILE_PATH as CLEANED_CSV_FILE_PATH
from sante_publique_france_off_enhancer.cleaning_filtering_features_products import \
    cleaning_and_filtering_of_features_and_products
from sante_publique_france_off_enhancer.file_loading import load_csv, save_csv
from src.sante_publique_france_off_enhancer.classes.bounds import *

logger = SingletonLogger.get_instance()
spark = SingletonSparkSession.get_instance()

CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')
CSV_FILE_NAME = "off_cleaned_and_filtered_abnormal_values_processed.csv"
CSV_FILE_PATH = os.path.join(CSV_DIR, CSV_FILE_NAME)


def boxplot_features_pyplot(df: pd.DataFrame, features_names: list[str], title: str):
    logger.debug(f"Creating boxplot for features using pyplot: {features_names}")

    df_ingredients_features = df[features_names]
    df_ingredients_features_melted = pd.melt(df_ingredients_features, var_name="Feature", value_name="Value")

    sns.boxplot(x="Feature", y="Value", data=df_ingredients_features_melted)
    plt.xticks(rotation=45)
    plt.title(title)
    plt.ylabel("Value")
    plt.show()

    logger.debug(f"Boxplot for features: {features_names} created with pyplot")


def boxplot_features_plotly(df: pd.DataFrame, features_names: list[str], title: str):
    logger.debug(f"Creating boxplot for features using plotly: {features_names}")

    df_filtered = df[features_names]

    fig = px.box(df_filtered, y=features_names, labels={'variable': 'Features', 'value': 'Value'}, title=title)

    fig.show()

    logger.debug(f"Boxplot for features: {features_names} created with plotly")


def bar_chart_pyplot(df: pd.DataFrame, feature_name: str, title: str):
    logger.debug(f"Creating bar chart for feature using pyplot: {feature_name}")

    category_counts = df[feature_name].value_counts()

    plt.figure(figsize=(15, 5))

    plt.bar(category_counts.index, category_counts.values)

    plt.title(title)
    plt.xlabel('Category')
    plt.ylabel('Frequency')

    plt.xticks(rotation=45)
    plt.show()

    logger.debug(f"Bar chart for feature: {feature_name} created with pyplot")


def bar_chart_plotly(df: pd.DataFrame, feature_name: str, title: str):
    logger.debug(f"Creating bar chart for feature using plotly: {feature_name}")

    fig = px.bar(df, x=feature_name, title=title,
                 labels={feature_name: 'Category', 'count': 'Frequency'},
                 text_auto=True)

    fig.update_layout(
        xaxis_title='Category',
        yaxis_title='Frequency',
        xaxis={'category order': 'total descending'}
    )

    fig.update_xaxes(tickangle=45)

    fig.show()

    logger.debug(f"Bar chart for feature: {feature_name} created with plotly")


def display_features_box_plots(df: pd.DataFrame):
    logger.debug("Displaying features box plots")

    nb_additives_feature_name = ["additives_n"]

    nb_ingredients_features_names = [
        "ingredients_from_palm_oil_n",
        "ingredients_that_may_be_from_palm_oil_n",
    ]

    energy_feature_name = ["energy_100g"]

    nutritional_features_names = [
        "fat_100g",
        "saturated-fat_100g",
        "carbohydrates_100g",
        "sugars_100g",
        "fiber_100g",
        "proteins_100g",
        "salt_100g",
        "sodium_100g",
        "fruits-vegetables-nuts_100g"
    ]

    nutriscore_feature_name = ["nutrition-score-fr_100g"]

    boxplot_features_plotly(df, nb_additives_feature_name, "Box plots for number of additives")
    boxplot_features_plotly(df, nb_ingredients_features_names, "Box plots for number of ingredients")
    boxplot_features_plotly(df, energy_feature_name, "Box plots for energy feature")
    boxplot_features_plotly(df, nutritional_features_names, "Box plots for nutritional features")
    boxplot_features_plotly(df, nutriscore_feature_name, "Boxplot for nutriscore feature")

    logger.debug("Features box plots displayed")


def display_features_bar_plots(df: pd.DataFrame):
    logger.debug("Displaying features bar plots")

    nutriscore_grade_feature_name = "nutrition_grade_fr"

    pnns_groups_1_feature_name = "pnns_groups_1"
    pnns_groups_2_feature_name = "pnns_groups_2"

    bar_chart_pyplot(df, nutriscore_grade_feature_name, "Boxplot for nutriscore grade feature")
    bar_chart_pyplot(df, pnns_groups_1_feature_name, "Boxplot for pnns main groups feature")
    bar_chart_pyplot(df, pnns_groups_2_feature_name, "Boxplot for pnns main subgroups feature")

    logger.debug("Features bar plots displayed")


def display_features_plots(df: DataFrame):
    pd_df = df.toPandas()

    display_features_box_plots(pd_df)
    display_features_bar_plots(pd_df)


def outlier_thresholds(df: DataFrame, column_name: str, default_lower_bound: float = 0) -> tuple[float, float]:
    logger.debug(f"Calculating outlier thresholds for column: {column_name}")

    # Calculate the first and third quartiles using approxQuantile
    q1, q3 = df.approxQuantile(column_name, [0.25, 0.75], 0)
    iqr = q3 - q1
    lower_bound = max(q1 - 1.5 * iqr, default_lower_bound)
    upper_bound = q3 + 1.5 * iqr

    logger.debug(
        f"Outlier thresholds for column: {column_name} calculated: "
        f"lower bound: {lower_bound}, upper bound: {upper_bound}"
    )

    return lower_bound, upper_bound


def filter_abnormal_values(bounds_list: list[Bounds], df: DataFrame) -> DataFrame:
    logger.debug("Filtering abnormal values")

    for bounds in bounds_list:
        df = df.withColumn(
            bounds.column,
            when((col(bounds.column) >= bounds.min) & (col(bounds.column) <= bounds.max), col(bounds.column)).otherwise(
                None)
        )

    logger.debug("Abnormal values filtered")

    return df


def filter_allowed_values(allowed_values: list[float | str], df: DataFrame, column: str) -> DataFrame:
    logger.debug(f"Filtering allowed values for column: {column}")

    df = df.withColumn(
        column,
        when(col(column).isin(allowed_values), col(column)).otherwise(None)
    )

    return df


def find_higher_bound(df: DataFrame, column: str, frequency_threshold: float) -> float:
    logger.debug(f"Finding higher bound for column: {column}")

    df_without_null = df.filter(df[column].isNotNull())

    frequencies_df = df_without_null.groupBy(column).count()

    total_count = df_without_null.count()
    frequencies_df = frequencies_df.withColumn('frequency', col('count') / total_count)

    frequencies_sorted = frequencies_df.sort(desc(column))

    cumulative_frequency = 0
    rows_data = []
    for row in frequencies_sorted.collect():
        cumulative_frequency += row['frequency']
        rows_data.append(Row(column=row[column], cumulative_frequency=cumulative_frequency))

    max_value = max(
         (float(row['column']) for row in rows_data if row['cumulative_frequency'] >= frequency_threshold),
         default=None
    )

    logger.debug(f"Higher bound for column: {column} found: {max_value}")

    if max_value is not None:
        return max_value
    else:
        # Fallback: Return the maximum value if no filtered values are found
        max_fallback = df.agg({column: 'max'}).collect()[0][0]
        return float(max_fallback)


@cache
def abnormal_values_processing(df: DataFrame) -> DataFrame:
    logger.info(f"Before processing abnormal values: {df.count()}")

    frequency_threshold = 0.01

    additives_n_higher_bound = find_higher_bound(df, "additives_n", frequency_threshold)
    ingredients_from_palm_oil_n_higher_bound = find_higher_bound(df, "ingredients_from_palm_oil_n",
                                                                 frequency_threshold)
    ingredients_that_may_be_from_palm_oil_n_higher_bound = find_higher_bound(df,
                                                                             "ingredients_that_may_be_from_palm_oil_n",
                                                                             frequency_threshold)

    nutrition_score_lower_bound, nutrition_score_higher_bound = outlier_thresholds(
        df,
        "nutrition_score_fr_100g",
        -20
    )

    bounds_list = [
        Bounds("additives_n", 0, additives_n_higher_bound),
        Bounds("ingredients_from_palm_oil_n", 0, ingredients_from_palm_oil_n_higher_bound),
        Bounds("ingredients_that_may_be_from_palm_oil_n", 0, ingredients_that_may_be_from_palm_oil_n_higher_bound),
        Bounds("fat_100g", 0, 100),
        Bounds("saturated_fat_100g", 0, 100),
        Bounds("sugars_100g", 0, 100),
        Bounds("salt_100g", 0, 100),
        Bounds("energy_100g", 0, 3700),
        Bounds("carbohydrates_100g", 0, 47),
        Bounds("fiber_100g", 0, 11),
        Bounds("proteins_100g", 0, 33),
        Bounds("sodium_100g", 0, 40),
        Bounds("nutrition_score_fr_100g", nutrition_score_lower_bound, nutrition_score_higher_bound),
        Bounds("fruits_vegetables_nuts_100g", 0, 100)
    ]

    allowed_values_nutritional_grade = ['a', 'b', 'c', 'd', 'e']

    filter_df = filter_abnormal_values(bounds_list, df)
    filter_df = filter_allowed_values(allowed_values_nutritional_grade, filter_df, "nutrition_grade_fr")

    logger.info(f"After processing abnormal values: {filter_df.count()}")

    return filter_df


if __name__ == "__main__":
    if os.path.exists(CLEANED_CSV_FILE_PATH):
        cleaned_and_filtered_df = load_csv(CLEANED_CSV_FILE_PATH)
    else:
        off_df = load_csv()
        cleaned_and_filtered_df = cleaning_and_filtering_of_features_and_products(off_df)

    cleaned_data_with_abnormal_values_processed = abnormal_values_processing(cleaned_and_filtered_df)

    save_csv(cleaned_data_with_abnormal_values_processed, CSV_FILE_PATH)
