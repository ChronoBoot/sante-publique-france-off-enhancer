"""## Step 2 : Identify and process abnormal values"""
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns

from src.cleaning_filtering_features_products import *
from src.classes.bounds import *


def boxplot_features_pyplot(df: pd.DataFrame, features_names: list[str], title: str):
    df_ingredients_features = df[features_names]
    df_ingredients_features_melted = pd.melt(df_ingredients_features, var_name="Feature", value_name="Value")

    sns.boxplot(x="Feature", y="Value", data=df_ingredients_features_melted)
    plt.xticks(rotation=45)
    plt.title(title)
    plt.ylabel("Value")
    plt.show()


def boxplot_features_plotly(df: pd.DataFrame, features_names: list[str], title: str):
    df_filtered = df[features_names]

    fig = px.box(df_filtered, y=features_names, labels={'variable': 'Features', 'value': 'Value'}, title=title)

    fig.show()


def bar_chart_pyplot(df: pd.DataFrame, feature_name: str, title: str):
    category_counts = df[feature_name].value_counts()

    plt.figure(figsize=(15, 5))

    plt.bar(category_counts.index, category_counts.values)

    plt.title(title)
    plt.xlabel('Category')
    plt.ylabel('Frequency')

    plt.xticks(rotation=45)
    plt.show()


def bar_chart_plotly(df: pd.DataFrame, feature_name: str, title: str):
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


def display_features_box_plots(df: pd.DataFrame):
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


def display_features_bar_plots(df: pd.DataFrame):
    nutriscore_grade_feature_name = "nutrition_grade_fr"

    pnns_groups_1_feature_name = "pnns_groups_1"
    pnns_groups_2_feature_name = "pnns_groups_2"

    bar_chart_pyplot(df, nutriscore_grade_feature_name, "Boxplot for nutriscore grade feature")
    bar_chart_pyplot(df, pnns_groups_1_feature_name, "Boxplot for pnns main groups feature")
    bar_chart_pyplot(df, pnns_groups_2_feature_name, "Boxplot for pnns main subgroups feature")


def outlier_thresholds(df: pd.DataFrame, column_name: str, default_lower_bound: float = 0) -> tuple[float, float]:
    q1 = df[column_name].quantile(0.25)
    q3 = df[column_name].quantile(0.75)
    iqr = q3 - q1
    lower_bound = max(q1 - 1.5 * iqr, default_lower_bound)
    upper_bound = q3 + 1.5 * iqr
    return lower_bound, upper_bound


def filter_abnormal_values(bounds_list: list[Bounds], df: pd.DataFrame) -> pd.DataFrame:
    for bounds in bounds_list:
        df[bounds.column] = df[(df[bounds.column] >= bounds.min) & (df[bounds.column] <= bounds.max)][
            bounds.column]
    return df


def filter_allowed_values(allowed_values: list[float | str], df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[df[column].isin(allowed_values)][column]
    return df


def find_higher_bound(df: pd.DataFrame, column: str, frequency_threshold: float) -> float:
    frequencies = df[column].value_counts(normalize=True).sort_index(ascending=False)

    cumulative_frequencies = frequencies.cumsum()
    filtered_values = cumulative_frequencies[cumulative_frequencies <= frequency_threshold]

    if not filtered_values.empty:
        return filtered_values.index[-1]
    else:
        return max(df[column])


def abnormal_values_processing(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df, features = cleaning_and_filtering_of_features_and_products(df)

    frequency_threshold = 0.01

    additives_n_higher_bound = find_higher_bound(cleaned_df, "additives_n", frequency_threshold)
    ingredients_from_palm_oil_n_higher_bound = find_higher_bound(cleaned_df, "ingredients_from_palm_oil_n",
                                                                 frequency_threshold)
    ingredients_that_may_be_from_palm_oil_n_higher_bound = find_higher_bound(cleaned_df,
                                                                             "ingredients_that_may_be_from_palm_oil_n",
                                                                             frequency_threshold)

    nutrition_score_lower_bound, nutrition_score_higher_bound = outlier_thresholds(
        cleaned_df,
        "nutrition-score-fr_100g",
        -20
    )

    bounds_list = [
        Bounds("additives_n", 0, additives_n_higher_bound),
        Bounds("ingredients_from_palm_oil_n", 0, ingredients_from_palm_oil_n_higher_bound),
        Bounds("ingredients_that_may_be_from_palm_oil_n", 0, ingredients_that_may_be_from_palm_oil_n_higher_bound),
        Bounds("fat_100g", 0, 100),
        Bounds("saturated-fat_100g", 0, 100),
        Bounds("sugars_100g", 0, 100),
        Bounds("salt_100g", 0, 100),
        Bounds("energy_100g", 0, 3700),
        Bounds("carbohydrates_100g", 0, 47),
        Bounds("fiber_100g", 0, 11),
        Bounds("proteins_100g", 0, 33),
        Bounds("sodium_100g", 0, 40),
        Bounds("nutrition-score-fr_100g", nutrition_score_lower_bound, nutrition_score_higher_bound),
        Bounds("fruits-vegetables-nuts_100g", 0, 100)
    ]

    allowed_values_nutritional_grade = ['a', 'b', 'c', 'd', 'e']

    filter_df = filter_abnormal_values(bounds_list, cleaned_df)
    filter_df = filter_allowed_values(allowed_values_nutritional_grade, filter_df, "nutrition_grade_fr")

    return filter_df


if __name__ == "__main__":
    off_df = load_csv()

    print(f"Before cleaning and filtering: {len(off_df)} products.")
    cleaned_data_with_abnormal_values_processed, cleaned_fields = (
        cleaning_and_filtering_of_features_and_products(off_df))
    print(f"After cleaning and filtering: {len(cleaned_data_with_abnormal_values_processed)} products.")

    cleaned_data_with_abnormal_values_processed = abnormal_values_processing(
        cleaned_data_with_abnormal_values_processed)
    print(f"After cleaning and filtering and abnormal value processing : "
          f"{len(cleaned_data_with_abnormal_values_processed)} products."
          )

    display_features_box_plots(cleaned_data_with_abnormal_values_processed)
    display_features_bar_plots(cleaned_data_with_abnormal_values_processed)
