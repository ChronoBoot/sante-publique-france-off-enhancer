import dataclasses
from pyspark.sql import DataFrame
from pyspark.sql.functions import when
from functools import reduce


#TODO apply filter
#TODO use median value
#TODO use KNN or linear regression => set the list of correlated variables

@dataclasses.dataclass
class NutritionFacts:
    appetizers = {'pnns_groups_2': 'Appetizers', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
                  'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    artificially_sweetened_beverages = {'pnns_groups_2': 'Artificially sweetened beverages', 'energy_100g': True,
                                        'saturated_fat_100g': True, 'sugars_100g': True, 'sodium_100g': True,
                                        'proteins_100g': True, 'fiber_100g': False, 'fruits_vegetables_nuts_100g': True}
    biscuits_and_cakes = {'pnns_groups_2': 'Biscuits and cakes', 'energy_100g': True, 'saturated_fat_100g': True,
                          'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True,
                          'fruits_vegetables_nuts_100g': True}
    bread = {'pnns_groups_2': 'Bread', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
             'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    breakfast_cereals = {'pnns_groups_2': 'Breakfast cereals', 'energy_100g': True, 'saturated_fat_100g': True,
                         'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True,
                         'fruits_vegetables_nuts_100g': True}
    cereals = {'pnns_groups_2': 'Cereals', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
               'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    cheese = {'pnns_groups_2': 'Cheese', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
              'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False, 'fruits_vegetables_nuts_100g': False}
    chocolate_products = {'pnns_groups_2': 'Chocolate products', 'energy_100g': True, 'saturated_fat_100g': True,
                          'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True,
                          'fruits_vegetables_nuts_100g': True}
    dairy_desserts = {'pnns_groups_2': 'Dairy desserts', 'energy_100g': True, 'saturated_fat_100g': True,
                      'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False,
                      'fruits_vegetables_nuts_100g': True}
    dressings_and_sauces = {'pnns_groups_2': 'Dressings and sauces', 'energy_100g': True, 'saturated_fat_100g': True,
                            'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False,
                            'fruits_vegetables_nuts_100g': True}
    dried_fruits = {'pnns_groups_2': 'Dried fruits', 'energy_100g': True, 'saturated_fat_100g': False,
                    'sugars_100g': True, 'sodium_100g': False, 'proteins_100g': True, 'fiber_100g': True,
                    'fruits_vegetables_nuts_100g': True}
    eggs = {'pnns_groups_2': 'Eggs', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': False,
            'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False, 'fruits_vegetables_nuts_100g': False}
    fats = {'pnns_groups_2': 'Fats', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': False,
            'sodium_100g': False, 'proteins_100g': False, 'fiber_100g': False, 'fruits_vegetables_nuts_100g': False}
    fish_and_seafood = {'pnns_groups_2': 'Fish and seafood', 'energy_100g': True, 'saturated_fat_100g': True,
                        'sugars_100g': False, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False,
                        'fruits_vegetables_nuts_100g': False}
    fruit_juices = {'pnns_groups_2': 'Fruit juices', 'energy_100g': True, 'saturated_fat_100g': False,
                    'sugars_100g': True, 'sodium_100g': False, 'proteins_100g': False, 'fiber_100g': False,
                    'fruits_vegetables_nuts_100g': True}
    fruit_nectars = {'pnns_groups_2': 'Fruit nectars', 'energy_100g': True, 'saturated_fat_100g': False,
                     'sugars_100g': True, 'sodium_100g': False, 'proteins_100g': False, 'fiber_100g': False,
                     'fruits_vegetables_nuts_100g': True}
    fruits = {'pnns_groups_2': 'Fruits', 'energy_100g': True, 'saturated_fat_100g': False, 'sugars_100g': True,
              'sodium_100g': False, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    ice_cream = {'pnns_groups_2': 'Ice cream', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
                 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    legumes = {'pnns_groups_2': 'Legumes', 'energy_100g': True, 'saturated_fat_100g': False, 'sugars_100g': False,
               'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    meat = {'pnns_groups_2': 'Meat', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': False,
            'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False, 'fruits_vegetables_nuts_100g': False}
    milk_and_yogurt = {'pnns_groups_2': 'Milk and yogurt', 'energy_100g': True, 'saturated_fat_100g': True,
                       'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True,
                       'fruits_vegetables_nuts_100g': True}
    non_sugared_beverages = {'pnns_groups_2': 'Non-sugared beverages', 'energy_100g': True, 'saturated_fat_100g': False,
                             'sugars_100g': False, 'sodium_100g': False, 'proteins_100g': False, 'fiber_100g': False,
                             'fruits_vegetables_nuts_100g': False}
    nuts = {'pnns_groups_2': 'Nuts', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
            'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    one_dish_meals = {'pnns_groups_2': 'One-dish meals', 'energy_100g': True, 'saturated_fat_100g': True,
                      'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True,
                      'fruits_vegetables_nuts_100g': True}
    pastries = {'pnns_groups_2': 'Pastries', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
                'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    pizza_pies_and_quiche = {'pnns_groups_2': 'Pizza pies and quiche', 'energy_100g': True, 'saturated_fat_100g': True,
                             'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True,
                             'fruits_vegetables_nuts_100g': True}
    potatoes = {'pnns_groups_2': 'Potatoes', 'energy_100g': True, 'saturated_fat_100g': False, 'sugars_100g': True,
                'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    processed_meat = {'pnns_groups_2': 'Processed meat', 'energy_100g': True, 'saturated_fat_100g': True,
                      'sugars_100g': False, 'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': False,
                      'fruits_vegetables_nuts_100g': False}
    salty_and_fatty_products = {'pnns_groups_2': 'Salty and fatty products', 'energy_100g': True,
                                'saturated_fat_100g': True, 'sugars_100g': True, 'sodium_100g': True,
                                'proteins_100g': True, 'fiber_100g': False, 'fruits_vegetables_nuts_100g': True}
    sandwich = {'pnns_groups_2': 'Sandwich', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
                'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    soups = {'pnns_groups_2': 'Soups', 'energy_100g': True, 'saturated_fat_100g': False, 'sugars_100g': True,
             'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    sweetened_beverages = {'pnns_groups_2': 'Sweetened beverages', 'energy_100g': True, 'saturated_fat_100g': False,
                           'sugars_100g': True, 'sodium_100g': True, 'proteins_100g': False, 'fiber_100g': False,
                           'fruits_vegetables_nuts_100g': True}
    sweets = {'pnns_groups_2': 'Sweets', 'energy_100g': True, 'saturated_fat_100g': True, 'sugars_100g': True,
              'sodium_100g': True, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}
    vegetables = {'pnns_groups_2': 'Vegetables', 'energy_100g': True, 'saturated_fat_100g': False, 'sugars_100g': True,
                  'sodium_100g': False, 'proteins_100g': True, 'fiber_100g': True, 'fruits_vegetables_nuts_100g': True}

    nutrient_filtering = [appetizers, artificially_sweetened_beverages, biscuits_and_cakes, bread, breakfast_cereals,
                          cereals, cheese, chocolate_products, dairy_desserts, dressings_and_sauces, dried_fruits,
                          eggs, fats, fish_and_seafood, fruit_juices, fruit_nectars, fruits, ice_cream, legumes, meat,
                          milk_and_yogurt, non_sugared_beverages, nuts, one_dish_meals, pastries,
                          pizza_pies_and_quiche, potatoes, processed_meat, salty_and_fatty_products, sandwich, soups,
                          sweetened_beverages, sweets, vegetables]

    @staticmethod
    def union_two_dfs(df1: DataFrame, df2: DataFrame) -> DataFrame:
        return df1.union(df2)

    @staticmethod
    def apply_nutrient_filtering(df: DataFrame):
        dfs = []
        for nutrient in NutritionFacts.nutrient_filtering:
            pnns_group_2 = nutrient['pnns_groups_2']
            df_filtered = df.filter(df['pnns_groups_2'] == pnns_group_2)
            for nutrient_name, should_keep in nutrient.items():
                if nutrient_name != 'pnns_groups_2' and not should_keep:
                    df_filtered = df_filtered.withColumn(nutrient_name, when(df[nutrient_name].isNotNull(), 0))
            dfs.append(df_filtered)
        return reduce(NutritionFacts.union_two_dfs, dfs)
