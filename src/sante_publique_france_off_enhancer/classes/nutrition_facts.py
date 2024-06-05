from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit
from functools import reduce



#TODO => look at values per pnns_groups_2 and define default values or thresholds
#TODO => define correlations between variables before using systems (salt if not link to anything apart salt)

class NutritionFacts:
    point_tables = {
        'energy_100g': [335, 670, 1005, 1340, 1675, 2010, 2345, 2680, 3015, 3350],
        'saturated_fat_100g': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'sugars_100g': [4.5, 9, 13.5, 18, 22.5, 27, 31, 36, 40, 45],
        'sodium_100g': [0.090, 0.180, 0.270, 0.360, 0.450, 0.540, 0.630, 0.720, 0.810, 0.900],
        'proteins_100g': [1.6, 3.2, 4.8, 6.4, 8.0],
        'fiber_100g': [0.9, 1.9, 2.8, 3.7, 4.7],
        'fruits_vegetables_nuts_100g': [40, 60, 80, 80, 80]
    }

    @staticmethod
    def get_nb_nutriscore_missing(df: DataFrame) -> int:
        return df.filter(df['nutrition_score_fr_100g'].isNull()).count()

    @staticmethod
    def calculate_points(df: DataFrame) -> DataFrame:
        for nutrient, thresholds in NutritionFacts.point_tables.items():
            column_point = when(col(nutrient).isNotNull() & (col(nutrient) < thresholds[0]), 0)

            for i, threshold in enumerate(thresholds):
                column_point = column_point.when(col(nutrient) >= threshold, i + 1)

            column_point = column_point.otherwise(lit(None))

            df = df.withColumn(f"{nutrient}_points", column_point)

        return df

    @staticmethod
    def calculate_nutriscore(df: DataFrame) -> DataFrame:
        df = NutritionFacts.calculate_points(df)

        negative_nutrients = [
            'energy_100g',
            'saturated_fat_100g',
            'sugars_100g',
            'sodium_100g'
        ]

        positive_nutrients = [
            'proteins_100g',
            'fiber_100g',
            'fruits_vegetables_nuts_100g'
        ]

        all_nutrients = negative_nutrients + positive_nutrients

        all_points_available = reduce(lambda a, b: a & b,
                                      (col(f"{nutrient}_points").isNotNull() for nutrient in all_nutrients))

        sum_negative = reduce(lambda a, b: a + b, (col(f"{nutrient}_points") for nutrient in negative_nutrients))
        sum_positive = reduce(lambda a, b: a + b, (col(f"{nutrient}_points") for nutrient in positive_nutrients))

        nutriscore = when(
            all_points_available,
            sum_negative - sum_positive
        ).otherwise(col("nutrition_score_fr_100g"))

        df = df.withColumn("nutrition_score_fr_100g", nutriscore)

        return df

    @staticmethod
    def filter_single_missing_nutrient(df: DataFrame) -> DataFrame:
        return df.filter(reduce(lambda a, b: a & b,
                            (col(f"{nutrient}_points").isNotNull() for nutrient in NutritionFacts.point_tables) +
                            [col("nutrition_score_fr_100g").isNotNull()]))

    @staticmethod
    def fill_missing_nutrient(df: DataFrame) -> DataFrame:
        df = NutritionFacts.calculate_points(df)
        df = NutritionFacts.filter_single_missing_nutrient(df)


        return df