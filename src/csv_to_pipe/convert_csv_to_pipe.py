import ast
import configparser
import pathlib
import re

from pyspark.sql import SparkSession, DataFrame


def read_csv(spark: SparkSession, source: str, delim: str = ",") -> DataFrame:
    """Read csv file with separator "delim" from "source" location into Spark DataFrame.
        For this use case we are using the function for two purposes:
            1. Reading csv_files to convert to "||" delim files
            2. Reading "||" delim files into Spark DataFrames

    Args:
        spark (SparkSession): SparkSession
        source (str): CSV location.
            For UC Volumes it takes the form /Volumes/<my_catalog>/<my_schema>/<my_volume>/<path>/<to>/<directory>
            For further information see https://docs.databricks.com/en/files/index.html
        delim (str, optional): Separator used in csv file. Defaults to ",".

    Returns:
        DataFrame: Spark DataFrame
    """
    df = (
        spark.read.format("csv")
        .option("delimiter", delim)
        .option("inferSchema", "True")
        .option("header", "True")
        .load(source)
    )
    return df


def write_csv(df: DataFrame, destination: str, delim: str = ",") -> None:
    """Write Spark DataFrame using the specified delimiter type.
        For this use case we are reading "," delim files and writing them back out as "||" delim files

    Args:
        df (DataFrame): Spark DataFrame
        destination (str): Path to save csv.
            For UC Volumes it takes the form /Volumes/<my_catalog>/<my_schema>/<my_volume>/<path>/<to>/<directory>.
            For further information see https://docs.databricks.com/en/files/index.html
        delim (str, optional): Separator used in csv file. Defaults to ",".
    """
    df.write.csv(
        path=destination,
        sep=delim,
        header=True,
        mode="overwrite",
    )


def save_to_table(df: DataFrame, destination: str) -> None:
    """Write a Spark DataFrame as a Delta Table to a Unity Catalog location

    Args:
        df (DataFrame): Spark DataFrame
        destination (str): Unity Catalog location in the form <catalog>.<schema>.<table>
    """
    df.write.format("delta").mode("overwrite").saveAsTable(destination)


def read_table(spark: SparkSession, table_location: str) -> DataFrame:
    """Read a Delta Table from a Unity Catalog location

    Args:
        spark (SparkSession): SparkSession
        table_location (str): Unity Catalog location in the form <catalog>.<schema>.<table>

    Returns:
        DataFrame: Spark DataFrame
    """
    df = spark.table(table_location)
    return df


def get_dbutils(spark: SparkSession):
    dbutils = None

    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)

    else:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


def csv_to_pipe() -> None:
    """Processes the uploaded data files. For each file located in the source path:
    1. Reads the "," csv file from Volumes into a Spark DataFrame
    2. Writes the Spark DataFrame as a "||" delimited file to Volumes
    3. Saves the Spark DataFrame as a Delta Table

    IF "data_cleanup" is set to TRUE in the config, then the files and tables will be removed
    """
    # Read "data_processing_config.ini" & parse configs
    data_processing_config_path = (
        pathlib.Path(__file__).parent / "convert_csv_to_pipe_config.ini"
    )
    data_processing_config = configparser.ConfigParser()
    data_processing_config.read(data_processing_config_path)

    data_cleanup = ast.literal_eval(data_processing_config["options"]["data_cleanup"])
    create_tables = ast.literal_eval(
        data_processing_config["options"]["create_test_table"]
    )
    data = ast.literal_eval(data_processing_config["paths"]["data"])

    # Get or Create SparkSession & dbutils
    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)

    # Removes pipe_delim files & tables IF "data_cleanup" is set to True in "data_processing_config.ini"
    if data_cleanup == True:
        for data_set in data:
            data_source = data_set["data_source"]
            data_destination = data_set["data_destination"]
            data_table_destination = data_set["table_destination"]

            # Remove "||" directory
            dbutils.fs.rm(data_destination, recurse=True)
            source_files = dbutils.fs.ls(data_source)
            for source_file in source_files:
                name = re.sub(
                    "[^a-zA-Z0-9]", "_", "_".join(source_file.name.split(".")[:-1])
                ).lower()
                table_path = f"{data_table_destination}.test_{name}"
                # Remove table
                try:
                    spark.sql(f"DROP TABLE {table_path}")
                    print(f"Dropped table: {table_path}")
                except:
                    print(f"Table Not Found: {table_path}")

    # Converts "," delim files to "||" delim files & if "create_tables" is set to true then it creates a table
    else:
        for data_set in data:
            data_source = data_set["data_source"]
            data_destination = data_set["data_destination"]
            data_table_destination = data_set["table_destination"]

            source_files = dbutils.fs.ls(data_source)
            for source_file in source_files:
                csv_path = source_file.path
                name = re.sub(
                    "[^a-zA-Z0-9]", "_", "_".join(source_file.name.split(".")[:-1])
                ).lower()
                pipe_delim_path = f"{data_destination}/{name}"
                table_path = f"{data_table_destination}.test_{name}"

                # Read csv
                df = read_csv(spark, csv_path)
                print(f"Read CSV File: {csv_path}")

                # Write csv
                write_csv(df, pipe_delim_path, "||")
                print(f'Created "||" delim file: {pipe_delim_path}')

                # Create table
                if create_tables == True:
                    save_to_table(df, table_path)
                    print(f"Created table: {table_path}")


if __name__ == "__main__":
    csv_to_pipe()
