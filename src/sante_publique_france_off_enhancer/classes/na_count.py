import dataclasses
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum


@dataclasses.dataclass
class NaCount:
    column: str
    sumNa: int = None

    def calculate_and_set_sum_na(self, df: DataFrame):
        self.sumNa = df.select(sum(col(self.column).isNull().cast("int"))).collect()[0][0]
