import dataclasses


@dataclasses.dataclass
class DataFrameField:
    name: str
    dtype: str = None
    na_percent: float = None
    field_type: str = None
    fill_percent: float = dataclasses.field(init=False)

    def __post_init__(self):
        self.fill_percent = 100 - self.na_percent if self.na_percent is not None else None

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

    def process_and_set_na_percent(self, col):
        self.na_percent = col.isna().mean() * 100
        self.fill_percent = 100 - self.na_percent
