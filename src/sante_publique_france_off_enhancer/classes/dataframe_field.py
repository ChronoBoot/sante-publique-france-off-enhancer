import dataclasses

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum

from sante_publique_france_off_enhancer.classes.singleton_logger import SingletonLogger

logger = SingletonLogger.get_instance()

@dataclasses.dataclass
class DataFrameField:
    name: str
    dtype: str = None
    na_percent: float = None
    field_type: str = None
    fill_percent: float = dataclasses.field(init=False)

    def __post_init__(self):
        self.fill_percent = 100 - self.na_percent if self.na_percent is not None else None


    @staticmethod
    def process_and_set_na_percent(dataframe_fields: 'list[DataFrameField]', df: DataFrame):
        logger.debug("Processing and setting NA percent")

        logger.debug("Counting total rows")
        total_rows = df.count()
        logger.debug("Total rows counted")

        # Create a new DataFrame that contains 1 if the column value is null and 0 otherwise
        df_nulls = df.select(*[when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df.columns])

        # Use the `agg` function to sum up the values in each column, which gives the count of nulls
        df_nulls_agg = df_nulls.agg(*[sum(c).alias(c) for c in df_nulls.columns])

        # Calculate the NA percentage for each column and set it in the corresponding DataFrameField object
        for row in df_nulls_agg.collect():
            for field in dataframe_fields:
                if field.name in row:
                    na_count = row[field.name]
                    na_percentage = (na_count / total_rows) * 100
                    field.na_percent = na_percentage

        logger.debug("NA percent processed and set")

    def display_info(self):
        """
        Print out the details of the DataFrameField instance.
        """
        print(f"Column Name: {self.name}")
        if self.dtype:
            print(f"Data Type: {self.dtype}")
        if self.na_percent is not None:
            print(f"Percentage of Missing Values: {self.na_percent}%")
        if self.fill_percent is not None:
            print(f"Percent of Non-Missing Values: {self.fill_percent}")
        if self.field_type:
            print(f"Field Type: {self.field_type}")
