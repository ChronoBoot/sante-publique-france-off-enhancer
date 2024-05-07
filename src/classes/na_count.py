import dataclasses
from pandas import DataFrame


@dataclasses.dataclass
class NaCount:
    column: str
    sumNa: int = None

    def calculate_and_set_sum_na(self, df: DataFrame):
        self.sumNa = df[self.column].isna().sum()
