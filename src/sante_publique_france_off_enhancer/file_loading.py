import os
from functools import cache

from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col
from sante_publique_france_off_enhancer.classes.singleton_logger import SingletonLogger
from sante_publique_france_off_enhancer.classes.singleton_spark_session import SingletonSparkSession

logger = SingletonLogger.get_instance()
spark = SingletonSparkSession.get_instance()


@cache
def load_csv():
    """Load the CSV file."""
    logger.info("Loading the CSV file")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    off_path = os.path.join(base_dir, '..', '..', 'csv', 'fr.openfoodfacts.org.products.csv')
    logger.debug(f"CSV file path: {off_path}")

    df = spark.read.csv(off_path, sep='\t', header=True, inferSchema=True)

    logger.info("CSV file loaded")
    logger.info(f"Number of products: {df.count()}")

    return df


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
