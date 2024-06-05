import os
from functools import cache

from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col
from sante_publique_france_off_enhancer.classes.singleton_logger import SingletonLogger
from sante_publique_france_off_enhancer.classes.singleton_spark_session import SingletonSparkSession

logger = SingletonLogger.get_instance()
spark = SingletonSparkSession.get_instance()

#TODO Look at how cache works => only in one session


@cache
def load_csv(file_path: str = None) -> DataFrame:
    """Load the CSV file."""
    if file_path is None:
        file_path = 'fr.openfoodfacts.org.products.csv'
        base_dir = os.path.dirname(os.path.abspath(__file__))
        off_path = os.path.join(base_dir, '..', '..', 'csv', file_path)
    else:
        off_path = file_path

    logger.info(f"Loading the CSV file {file_path}")

    df = spark.read.csv(off_path, sep='\t', header=True, inferSchema=True)

    logger.info(f"CSV file loaded {file_path}")
    logger.info(f"Number of products: {df.count()}")

    return df


def save_csv(df: DataFrame, path: str):
    """Save the DataFrame to a CSV file."""
    logger.info(f"Saving the DataFrame to a CSV file: {path}")

    pandas_df = df.toPandas()

    pandas_df.to_csv(path, sep='\t', header=True, index=False)

    logger.info("DataFrame saved to a CSV file")

    return path


def print_info(df: DataFrame):
    """Print the number of products and the DataFrame info."""
    logger.debug("Printing the number of products and the DataFrame info")

    print(f"Number of products {df.count()}")

    # Get the schema information with column names and data types
    schema_info = [(field.name, field.dataType) for field in df.schema.fields]
    print("Schema Information:", schema_info)

    # Creating a dictionary to hold non-null counts
    non_null_counts = df.select([count(col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
    print("Non-null Counts:", non_null_counts)

    logger.debug(f"Number of products and DataFrame info printed: {df.count()}")


if __name__ == '__main__':
    off_df = load_csv()
    print_info(off_df)
