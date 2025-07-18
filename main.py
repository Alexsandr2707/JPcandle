import argparse

import multiprocessing
from datetime import datetime

from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

import os
import glob
import shutil

import xml.etree.ElementTree as ET

# Time formats
# suffix SPARK/PYTHON indicate which library these formats are for

# Input/output table time format
TIMESTAMP_INPUT_SPARK = "yyyyMMddHHmmssSSS"
TIMESTAMP_OUTPUT_SPARK = "yyyyMMddHHmmssSSS"

# Config input time format
DATE_FORMAT_INPUT_PYTHON = "%Y%m%d"
HHMM_FORMAT_INPUT_PYTHON = "%H%M"
# Date + Time
DT_INPUT_PYTHON = DATE_FORMAT_INPUT_PYTHON + HHMM_FORMAT_INPUT_PYTHON

# Internal represantation
DATE_FORMAT_PYTHON = "%Y-%m-%d"
HHMM_FORMAT_PYTHON = "%H:%M"
TIMESTAMP_PYTHON = DATE_FORMAT_PYTHON + " " + HHMM_FORMAT_PYTHON

DATE_FORMAT_SPARK = "yyyy-MM-dd"
HHMM_FORMAT_SPARK = "HH:mm"
TIMESTAMP_SPARK = DATE_FORMAT_SPARK + " " + HHMM_FORMAT_SPARK

# Num of aviable cores
NUM_CORES = multiprocessing.cpu_count()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("database-csv",
                        help="Path database written in CSV file (required)")
    parser.add_argument("--config", help="Run cofiguration")
    parser.add_argument("--outdir",
                        default="./output",
                        help="Directory for results")
    parser.add_argument("--clean-output-dir",
                        action="store_true",
                        help="Remove and create outdir")
    return vars(parser.parse_args())


class CandleConfig():

    def __init__(
            self,
            params: dict = {},
            width_ms="300000",
            date_from="19000101",  # yyyyMMdd
            date_to="20200101",  # yyyyMMdd
            time_from="1000",  # HHmm
            time_to="1800"):  # HHmm

        width_ms = str(params.get("candle.width", width_ms))
        date_from = str(params.get("candle.date.from", date_from))
        date_to = str(params.get("candle.date.to", date_to))
        time_from = str(params.get("candle.time.from", time_from))
        time_to = str(params.get("candle.time.to", time_to))

        # Need to add checks
        # Time convertions
        self.width_ms = int(width_ms)

        convert_time = lambda time, old_format, new_format: \
            datetime.strptime(time, old_format).strftime(new_format)

        self.date_from = convert_time( \
            date_from,
            DATE_FORMAT_INPUT_PYTHON,
            DATE_FORMAT_PYTHON
        )

        self.date_to = convert_time( \
            date_to,
            DATE_FORMAT_INPUT_PYTHON,
            DATE_FORMAT_PYTHON
        )

        self.time_from = convert_time( \
            time_from,
            HHMM_FORMAT_INPUT_PYTHON,
            HHMM_FORMAT_PYTHON
        )

        self.time_to = convert_time( \
            time_to,
            HHMM_FORMAT_INPUT_PYTHON,
            HHMM_FORMAT_PYTHON
        )

        self.timestamp_to = convert_time( \
            date_to + time_to,
            DT_INPUT_PYTHON,
            TIMESTAMP_PYTHON
        )

        self.timestamp_from = convert_time( \
            date_from + time_from,
            DT_INPUT_PYTHON,
            TIMESTAMP_PYTHON
        )


class Config():

    def __init__(
        self,
        args: dict,
    ):
        self.config_xml_file = args.get("config")
        self.params = self._parse_xml_file(self.config_xml_file)
        self.candle_config = CandleConfig(self.params)
        self.database_csv_path = args.get("database-csv")
        self.outdir = args.get("outdir")
        self.clean_output_dir = args.get("clean_output_dir")

    def _parse_xml_file(self, file) -> dict:
        if file == None:
            return dict()

        tree = ET.parse(file)
        root = tree.getroot()

        config = {}

        for property in root:
            name = property.find("name").text
            config[name] = property.find("value").text
        return config


#
# SCHEME OF CSV DATABASE
#
TRADE_SCHEMA = StructType([
    StructField("#SYMBOL", StringType(), nullable=False),
    StructField("SYSTEM", StringType(), nullable=True),
    StructField("MOMENT", TimestampType(), nullable=False),
    StructField("ID_DEAL", IntegerType(), nullable=False),
    StructField("PRICE_DEAL", DecimalType(15, 4), nullable=False),
    StructField("VOLUME", IntegerType(), nullable=True),
    StructField("OPEN_POS", IntegerType(), nullable=True),
    StructField("DIRECTION", StringType(), nullable=True),
])


def get_trades_dataframe(spark: SparkSession, config: Config) -> DataFrame:

    trades = spark.read.csv(config.database_csv_path,
                            schema=TRADE_SCHEMA,
                            header=True,
                            timestampFormat=TIMESTAMP_INPUT_SPARK)

    trades.repartition(NUM_CORES)

    return trades


def calculate_candles(trades: DataFrame,
                      candle_config: CandleConfig) -> DataFrame:

    # Drop records, where MOMENT not in [timestamp_from, timestamp_to)
    trades = trades.filter( \
        (F.date_format("MOMENT", TIMESTAMP_SPARK) >= F.lit(candle_config.timestamp_from)) &
        (F.date_format("MOMENT", TIMESTAMP_SPARK) < F.lit(candle_config.timestamp_to))
    )

    # Calculate timestamp of begin of candle
    trades = trades \
        .withColumn(
            "CANDLE_MOMENT",
            F.timestamp_millis(
                F.unix_millis(
                    F.date_trunc("day", "MOMENT")) + (
                    (
                        F.hour("MOMENT") * 3600 * 1000 +
                        F.minute("MOMENT") * 60 * 1000 +
                        F.second("MOMENT") * 1000 +
                        F.date_format("MOMENT", "SSS").cast("int")
                    )
                    / candle_config.width_ms
                ).cast("long") * candle_config.width_ms
            )
        )

    # Repartition to ensure that records corresponding
    # to the same candle are in the same partition
    trades.repartition(NUM_CORES, "#SYMBOL", "CANDLE_MOMENT")

    # Calculate candles
    window_spec = Window \
        .partitionBy("#SYMBOL", "CANDLE_MOMENT") \
        .orderBy("MOMENT", "ID_DEAL")

    # Find first and last trade
    trade_stats = trades.select(
        "#SYMBOL", \
        "CANDLE_MOMENT", \
        "PRICE_DEAL", \
        F.first("PRICE_DEAL").over(window_spec).alias("OPEN"), \
        F.last("PRICE_DEAL").over(window_spec).alias("CLOSE")
    )

    # Final candles
    candles = trade_stats \
    .groupBy(
        F.col("#SYMBOL").alias("SYMBOL"),
        F.col("CANDLE_MOMENT").alias("MOMENT")) \
    .agg(
        F.first("OPEN").alias("OPEN"),
        F.max("PRICE_DEAL").alias("HIGH"),
        F.min("PRICE_DEAL").alias("LOW"),
        F.last("CLOSE").alias("CLOSE")
    )

    return candles


def format_candle(candles: DataFrame) -> DataFrame:
    candles = candles \
        .withColumn("MOMENT", F.date_format("MOMENT", TIMESTAMP_OUTPUT_SPARK)) \
        .withColumn("OPEN", F.round("OPEN", 1)) \
        .withColumn("HIGH", F.round("HIGH", 1)) \
        .withColumn("LOW", F.round("LOW", 1)) \
        .withColumn("CLOSE", F.round("CLOSE", 1))
    return candles


def save_candles(candles: DataFrame, config: Config):

    candles = format_candle(candles)
    symbols = [
        row["SYMBOL"] for row in candles.select("SYMBOL").distinct().collect()
    ]

    if not os.path.isdir(config.outdir):
        os.mkdir(config.outdir)

    if config.clean_output_dir:
        shutil.rmtree(config.outdir)
        os.mkdir(config.outdir)

    for symbol in symbols:
        # Collect and write to files
        candels_symbol = candles.filter(F.col("SYMBOL") == symbol).coalesce(1)

        candels_symbol.write \
                    .option("header", "false") \
                    .mode("overwrite") \
                    .csv(f"{config.outdir}/tmp_{symbol}")

        # Rename file
        tmp_file = glob.glob(f"{config.outdir}/tmp_{symbol}/part-*.csv")[0]
        os.rename(tmp_file, f"{config.outdir}/{symbol}.csv")
        shutil.rmtree(f"{config.outdir}/tmp_{symbol}")

    pass


def make_candles(config: Config):
    # Config SparkSession
    spark = SparkSession.builder.appName("Candle Calculating").getOrCreate()

    # PySpark version >= 3.0  does not allow parsing time written without delimiters
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Creation dataframe
    trades = get_trades_dataframe(spark, config)

    # Calculate candles (lazy calculations)
    candles = calculate_candles(trades, config.candle_config)

    # Calculation and saving
    save_candles(candles, config)

    pass


if __name__ == "__main__":
    args = parse_args()
    config = Config(args)

    make_candles(config)
