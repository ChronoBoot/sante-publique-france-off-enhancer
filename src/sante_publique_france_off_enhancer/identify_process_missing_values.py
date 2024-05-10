"""## Step 3 : Identify and process missing values"""
from src.sante_publique_france_off_enhancer.identify_process_abnormal_values import *
from src.sante_publique_france_off_enhancer.classes.nutrition_facts import NutritionFacts
from src.sante_publique_france_off_enhancer.classes.na_count import NaCount


def get_na_counts(df: DataFrame) -> list[NaCount]:
    fields = df.columns

    na_counts = [NaCount(col) for col in fields]

    for na_count in na_counts:
        na_count.calculate_and_set_sum_na(df)

    return na_counts


def na_counts_to_dataframe(na_counts: list[NaCount]) -> DataFrame:
    data = [{"name": na_count.column, 'sumNa': na_count.sumNa} for na_count in na_counts]
    df = DataFrame(data)
    df.set_index('name', inplace=True)

    return df


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


def is_beverage_category(row: pd.Series) -> bool:
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


def df_calculate_nutriscore(row: pd.Series) -> float | None:
    if row[['energy_100g', 'saturated-fat_100g', 'sugars_100g',
            'sodium_100g', 'proteins_100g', 'fiber_100g', 'fruits-vegetables-nuts_100g']].isnull().any():
        return None

    nutrition_facts = NutritionFacts(
        row['energy_100g'],
        row['saturated-fat_100g'],
        row['sugars_100g'],
        row['sodium_100g'],
        row['proteins_100g'],
        row['fiber_100g'],
        row['fruits-vegetables-nuts_100g']
    )

    # Call a method to calculate the nutri-score or similar metric
    return nutrition_facts.calculate_nutriscore()


def fill_missing_nutritional_fact(row: pd.Series) -> pd.Series:
    nutritional_facts = NutritionFacts.row_to_nutrition_facts(row)

    if pd.isna(row['nutrition-score-fr_100g']) and nutritional_facts.get_nb_attributes_missing() == 0:
        row['nutrition-score-fr_100g'] = nutritional_facts.calculate_nutriscore()
    elif nutritional_facts.get_nb_attributes_missing() == 1 and not pd.isna(row['nutrition-score-fr_100g']):
        nutritional_facts.solve_for_missing_nutrient(row['nutrition-score-fr_100g'])

    return NutritionFacts.nutrition_facts_to_row(nutritional_facts, row)


def fill_additives(row: pd.Series) -> pd.Series:
    if row['additives_n'] is None:
        row['additives_n'] = row['median_additives_n']
    if row['ingredients_from_palm_oil_n'] is None:
        row['ingredients_from_palm_oil_n'] = row['median_ingredients_from_palm_oil_n']
    if row['ingredients_that_may_be_from_palm_oil_n'] is None:
        row['ingredients_that_may_be_from_palm_oil_n'] = row['median_ingredients_that_may_be_from_palm_oil_n']

    return row


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


def fill_missing_values_with_pnns_groups_2_median(row: pd.Series) -> pd.Series:
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
        'fruits-vegetables-nuts_100g'
    ]

    for column in columns:
        if row[column] is None:
            row[column] = row[f"median_{column}"]

    return row


if __name__ == '__main__':
    off_df = load_csv()
    cleaned_data_with_abnormal_values_processed = abnormal_values_processing(off_df).head(100)

    print(f"Before filling missing values, number of nutriscore missing: "
          f"{cleaned_data_with_abnormal_values_processed['nutrition-score-fr_100g'].isna().sum()}")

    cleaned_data_with_abnormal_values_processed = (
        cleaned_data_with_abnormal_values_processed.apply(fill_missing_nutritional_fact, axis=1))

    print(f"After filling missing values, number of nutriscore missing: "
          f"{cleaned_data_with_abnormal_values_processed['nutrition-score-fr_100g'].isna().sum()}")

    # cleaned_data_with_abnormal_values_processed = set_median_values(cleaned_data_with_abnormal_values_processed)
    #
    # cleaned_data_with_abnormal_values_processed = (
    #     cleaned_data_with_abnormal_values_processed.apply(fill_missing_values_with_pnns_groups_2_median, axis=1))
