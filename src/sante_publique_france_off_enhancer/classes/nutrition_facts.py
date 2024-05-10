import dataclasses

import numpy as np
from scipy.optimize import fsolve


@dataclasses.dataclass
class NutritionFacts:
    energy: float = None
    sat_fat: float = None
    sugars: float = None
    sodium: float = None
    protein: float = None
    fiber: float = None
    fruits_vegetables_nuts: float = None
    energy_points: float = dataclasses.field(init=False, default=None)
    sat_fat_points: float = dataclasses.field(init=False, default=None)
    sugars_points: float = dataclasses.field(init=False, default=None)
    sodium_points: float = dataclasses.field(init=False, default=None)
    protein_points: float = dataclasses.field(init=False, default=None)
    fiber_points: float = dataclasses.field(init=False, default=None)
    fruits_vegetables_nuts_points: float = dataclasses.field(init=False, default=None)

    def __post_init__(self):
        self.update_all_points()

    def update_all_points(self):
        for nutrient in self.nutrient_names:
            self.update_points(nutrient)

    @property
    def nutrient_names(self):
        return [
            'energy', 'sat_fat', 'sugars', 'sodium', 'protein', 'fiber', 'fruits_vegetables_nuts'
        ]

    def update_points(self, nutrient):
        points_table = self.get_points_table(nutrient)
        value = getattr(self, nutrient)
        points = self.calculate_points(value, points_table) if value is not None else None
        setattr(self, f"{nutrient}_points", points)

    @staticmethod
    def get_points_table(nutrient):
        point_tables = {
            'energy': [335, 670, 1005, 1340, 1675, 2010, 2345, 2680, 3015, 3350],
            'sat_fat': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sugars': [4.5, 9, 13.5, 18, 22.5, 27, 31, 36, 40, 45],
            'sodium': [0.090, 0.180, 0.270, 0.360, 0.450, 0.540, 0.630, 0.720, 0.810, 0.900],
            'protein': [1.6, 3.2, 4.8, 6.4, 8.0],
            'fiber': [0.9, 1.9, 2.8, 3.7, 4.7],
            'fruits_vegetables_nuts': [40, 60, 80, 80, 80]
        }
        return point_tables[nutrient]

    @staticmethod
    def calculate_points(value, points_table):
        return next((index for index, point in enumerate(points_table) if point >= value), len(points_table))

    @staticmethod
    def row_to_nutrition_facts(row):
        return NutritionFacts(
            energy=row.get('energy_100g', None),
            sat_fat=row.get('saturated-fat_100g', None),
            sugars=row.get('sugars_100g', None),
            sodium=row.get('sodium_100g', None),
            protein=row.get('proteins_100g', None),
            fiber=row.get('fiber_100g', None),
            fruits_vegetables_nuts=row.get('fruits-vegetables-nuts_100g', None),
        )

    @staticmethod
    def nutrition_facts_to_row(nutrition_facts, row):
        for nutrient in nutrition_facts.nutrient_names:
            row[f'{nutrient}_100g'] = getattr(nutrition_facts, nutrient)
            row[f'{nutrient}_100g_points'] = getattr(nutrition_facts, f'{nutrient}_points')
        return row

    def find_all_missing(self):
        return [nutrient for nutrient in self.nutrient_names if getattr(self, nutrient) is None]

    def get_nb_attributes_missing(self):
        return len(self.find_all_missing())

    """Find the missing nutritional fact. Should only be called when there is only one missing fact."""
    def find_missing(self):
        missing = self.find_all_missing()
        if len(missing) == 1:
            return missing[0]
        elif not missing:
            raise ValueError("No missing nutritional facts.")
        else:
            raise ValueError("Multiple missing nutritional facts, ensure only one is missing for calculation.")

    def calculate_nutriscore(self):
        negative_points = sum(
            filter(
                None, [self.energy_points, self.sat_fat_points, self.sugars_points, self.sodium_points]
            )
        )
        positive_points = sum(
            filter(
                None, [self.protein_points, self.fiber_points, self.fruits_vegetables_nuts_points]
            )
        )
        return negative_points - positive_points

    def solve_for_missing_nutrient(self, target_score):
        missing_attr = self.find_missing()
        points_table = self.get_points_table(missing_attr)
        min_diff = np.inf
        points = 0

        for i in range(0, len(points_table)):
            setattr(self, missing_attr, i)
            result = self.calculate_nutriscore()
            diff = abs(result - target_score)
            if diff < min_diff:
                min_diff = diff
                points = i

        # We take the average value of the nutrient based on the table for the given point
        if points == 0:
            missing_attr_value = points_table[points]
        else:
            missing_attr_value = (points_table[points-1] + points_table[points])/2

        setattr(self, missing_attr, missing_attr_value)
        self.update_all_points()
        return {missing_attr: missing_attr_value}
