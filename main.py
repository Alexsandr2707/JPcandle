import argparse

import logging.handlers
import multiprocessing
from datetime import datetime

from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

import os
import glob
import shutil
from pathlib import Path

import xml.etree.ElementTree as ET

import logging
import time

# Logging configuration
DEF_LOG_DIR = "logs"
DEF_LOG_FILE = "app.log"
DEF_LOG_FILE_SIZE = 10 * 1024 * 1024  # 10Mb
DEF_BACKUP_COUNT = 5
DEF_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def setup_logger(log_dir: Path = Path(DEF_LOG_DIR)):
    # Create log directoy
    try:
        log_dir.mkdir(exist_ok=True, parents=True)
    except Exception as e:
        print("Fatal error in creating log directory: %s", e)
        raise

    # Create log file and check on write access
    try:
        log_file = log_dir.joinpath(DEF_LOG_FILE)
        with open(log_file, "w") as f:
            f.write("Something")
        os.remove(log_file)

    except Exception as e:
        print("Fatal error in creating log file: %s", e)
        raise

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(DEF_LOG_FORMAT)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler( \
        log_file,
        maxBytes=DEF_LOG_FILE_SIZE,
        backupCount=DEF_BACKUP_COUNT,
        encoding="utf-8"
    )

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()

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
MIN_PARTION_COUNT = NUM_CORES


def try_remove_dir(dir: str):
    for i in range(3):
        try:
            shutil.rmtree(dir)
            break
        except:
            time.sleep(0.5)
    else:
        raise RuntimeError("Can't remove %s directory", dir)


def parse_args():
    try:
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
        parser.add_argument("--sort-by-moment",
                            action="store_true",
                            help="Sort candles in files by time begin")
        args_dict = vars(parser.parse_args())

    except Exception as e:
        logger.exception("Error parsing command line arguments")
        raise

    logger.info("Command line args parsed successfully")
    logger.debug("Command line args: %s", args_dict)

    return args_dict


class CandleConfig():

    def __init__(
            self,
            params: dict = {},
            width_ms="300000",
            date_from="19000101",  # yyyyMMdd
            date_to="20200101",  # yyyyMMdd
            time_from="1000",  # HHmm
            time_to="1800"  # HHmm
    ):

        try:
            width_ms = str(params.get("candle.width", width_ms))
            date_from = str(params.get("candle.date.from", date_from))
            date_to = str(params.get("candle.date.to", date_to))
            time_from = str(params.get("candle.time.from", time_from))
            time_to = str(params.get("candle.time.to", time_to))

            # Need to add checks
            # Time convertions
            self.width_ms = int(width_ms)

            if self.width_ms <= 0:
                raise ValueError("Width of candle <= 0")

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

        except Exception as e:
            logger.exception("Error in candle parameters processing\n   %s", e)
            raise

        logger.info("Candle parameters processed successfully")
        logger.debug( \
            "Candle parameters: width_ms=%s, date_from=%s, date_to=%s, time_from=%s, time_to=%s",
            self.width_ms,
            self.date_from, self.date_to,
            self.time_from, self.time_to
        )


class Config():

    def __init__(
        self,
        args: dict,
    ):
        self.config_xml_file = args.get("config")
        if not self.config_xml_file:
            logger.warning("XML configuration file not specified")

        params = self._parse_xml_file(self.config_xml_file)
        self.candle_config = CandleConfig(params)
        self.database_csv_path = Path(args.get("database-csv"))
        self.outdir = Path(args.get("outdir"))
        self.clean_output_dir = args.get("clean_output_dir")
        self.sort_by_moment = args.get("sort_by_moment")

        if not self.database_csv_path.exists():
            raise FileExistsError("Incorrect path to database",
                                  self.database_csv_path)

        logger.info("Config created successfully")
        logger.debug( \
            "Config parameters: candle_config=..., database_csv_path=%s, outdir=%s, clean_output_dir=%s, sort_by_moment=%s",
            self.database_csv_path,
            self.outdir,
            self.clean_output_dir,
            self.sort_by_moment
        )

    def _parse_xml_file(self, file) -> dict:
        if file == None:
            return dict()

        try:
            tree = ET.parse(file)
            root = tree.getroot()

            config = {}

            for property in root:
                name = property.find("name").text
                config[name] = property.find("value").text

        except Exception as e:
            logger.exception("XML configuration file parsing error")
            raise

        logger.info("XML configuration file parsed successfully")
        logger.debug("XML configuration file parameters: %s", config)
        return config


def config_spark_session() -> SparkSession:
    # Config SparkSession
    try:
        spark = SparkSession.builder.appName("Candle Calculating").getOrCreate()

        # PySpark version >= 3.0  does not allow parsing time written without delimiters
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    except Exception as e:
        logger.exception("Spark Configuration Error %s", e)
        raise

    logger.info("Spark configurated successfully")
    return spark


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

    trades = spark.read.csv(str(config.database_csv_path),
                            schema=TRADE_SCHEMA,
                            header=True,
                            timestampFormat=TIMESTAMP_INPUT_SPARK)

    trades = trades.repartition(MIN_PARTION_COUNT)

    return trades


# Return candle dataframe with columns:
# SYMBOL, MOMENT, OPEN, HIGH, LOW, CLOSE
def calculate_candles(trades: DataFrame,
                      candle_config: CandleConfig) -> DataFrame:

    # Drop records, where MOMENT not in [timestamp_from, timestamp_to)
    trades = trades.filter( \
        (F.date_format("MOMENT", DATE_FORMAT_SPARK) >= F.lit(candle_config.date_from)) &
        (F.date_format("MOMENT", DATE_FORMAT_SPARK) < F.lit(candle_config.date_to)) &
        (F.date_format("MOMENT", HHMM_FORMAT_SPARK) >= F.lit(candle_config.time_from)) &
        (F.date_format("MOMENT", HHMM_FORMAT_SPARK) < F.lit(candle_config.time_to))
    )

    # Calculate timestamp of begin of candle
    trades = trades \
        .withColumn(
            "CANDLE_MOMENT",
            # Timestamp of candle begin
            F.timestamp_millis(
                # Time of candle begin in milliseconds
                F.unix_millis(F.date_trunc("day", "MOMENT")) +
                ((  # Get milliseconds count from begin of day
                    F.hour("MOMENT") * 3600 * 1000 +
                    F.minute("MOMENT") * 60 * 1000 +
                    F.second("MOMENT") * 1000 +
                    F.date_format("MOMENT", "SSS").cast("int")
                ) / candle_config.width_ms).cast("long") * candle_config.width_ms
            )
        )

    # Repartition to ensure that records corresponding
    # to the same candle are in the same partition
    trades = trades.repartition(MIN_PARTION_COUNT, "#SYMBOL", "CANDLE_MOMENT")

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
        .withColumn("SYMBOL", F.trim("SYMBOL")) \
        .withColumn("MOMENT", F.trim(F.date_format("MOMENT", TIMESTAMP_OUTPUT_SPARK))) \
        .withColumn("OPEN", F.round("OPEN", 1)) \
        .withColumn("HIGH", F.round("HIGH", 1)) \
        .withColumn("LOW", F.round("LOW", 1)) \
        .withColumn("CLOSE", F.round("CLOSE", 1))

    return candles


def save_candles(candles: DataFrame, config: Config):
    try:
        symbols = [
            row["SYMBOL"]
            for row in candles.select("SYMBOL").distinct().collect()
        ]

        config.outdir.mkdir(exist_ok=True, parents=True)

        if config.clean_output_dir:
            try_remove_dir(config.outdir)
            config.outdir.mkdir(exist_ok=True, parents=True)

        for symbol in symbols:
            # Collect and write to files
            candles_symbol = candles.filter( \
                F.col("SYMBOL") == symbol
            ).coalesce(1)

            if config.sort_by_moment:
                candles_symbol = candles_symbol.orderBy("MOMENT")

            candles_symbol.write \
                        .option("header", "false") \
                        .mode("overwrite") \
                        .csv(f"{config.outdir}/tmp_{symbol}")

            # Rename file
            tmp_file = glob.glob(f"{config.outdir}/tmp_{symbol}/part-*.csv")[0]
            os.rename(tmp_file, f"{config.outdir}/{symbol}.csv")

            try_remove_dir(f"{config.outdir}/tmp_{symbol}")

    except Exception as e:
        logger.exception("Save or calculation candles error: %s", e)
        raise

    logger.info("Candles saved successfully")
    pass


def make_candles(config: Config):
    # Config SparkSession
    spark = config_spark_session()

    # Creation dataframe
    trades = get_trades_dataframe(spark, config)

    # Calculate candles (lazy calculations)
    candles = calculate_candles(trades, config.candle_config)

    # Format candles (lazy calculations)
    candles = format_candle(candles)

    # Caching before calculations
    candles = candles.cache()

    # Calculation and saving
    logger.info("Start candle calculations")

    start_time = time.time()
    try:
        save_candles(candles, config)
    except:
        logger.exception("Candle calculation interrupted")
        raise
    finally:
        end_time = time.time()
        logger.info("Candle calculation completed in %.3f seconds",
                    end_time - start_time)

    spark.stop()
    pass


if __name__ == "__main__":
    try:
        logger.info("Start application")

        args = parse_args()
        config = Config(args)

        make_candles(config)

        logger.info("Application finished successfully")

    except Exception as e:
        logger.exception("Application terminated with error")
        exit(1)
