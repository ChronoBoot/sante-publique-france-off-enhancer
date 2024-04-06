class DataFrameField:
    @staticmethod
    def process_na_percent(col, set_na_percent=False):
      na_percent = col.isna().mean() * 100
      return na_percent

    def __init__(self, name, dtype=None, na_percent=None, field_type=None):
        """
        Initialize a DataFrameField object with only the name required at creation.
        Other attributes can be set later.

        Parameters:
        - name (str): The name of the DataFrame column.
        - dtype (str, optional): The data type of the column. Defaults to None.
        - na_percent (float, optional): The percentage of missing (NA) values in the column. Defaults to None.
        - field_type (str, optional): Specifies whether the field is 'categorical' or 'numerical'. Defaults to None.
        """
        self.name = name
        self.dtype = dtype
        self.na_percent = na_percent
        # The percentage of non-mising values in the column
        self.fill_percent = 100 - na_percent if na_percent is not None else None
        self.field_type = field_type

    def __repr__(self):
        """
        Provide a string representation of the DataFrameField instance.
        """
        return (f"DataFrameField(name='{self.name}', dtype='{self.dtype}', "
                f"na_percent='{self.na_percent}', fill_percent='{self.fill_percent}',"
                f"field_type='{self.field_type}')")

    def display_info(self):
        """
        Print out the details of the DataFrameField instance.
        """
        print(f"Column Name: {self.name}")
        if self.dtype:
            print(f"Data Type: {self.dtype}")
        if self.na_percent is not None:
            print(f"Percentage of Missing Values: {self.na_percent}%")
        if self.fill_percent is not None :
            print(f"Percent of Non-Missing Values: {self.fill_percent}")
        if self.field_type:
            print(f"Field Type: {self.field_type}")

    # Additional methods to set attributes after object creation
    def set_dtype(self, dtype):
        self.dtype = dtype

    def set_na_percent(self, na_percent):
        self.na_percent = na_percent
        self.fill_percent = 100 - na_percent

    def set_field_type(self, field_type):
        self.field_type = field_type

    def process_and_set_na_percent(self, col):
        na_percent = DataFrameField.process_na_percent(col)
        self.set_na_percent(na_percent)

