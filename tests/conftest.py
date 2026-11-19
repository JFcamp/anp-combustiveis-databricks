import os
import sys

import pytest
from pyspark.sql import SparkSession

# Driver e workers precisam usar o mesmo Python (evita PYTHON_VERSION_MISMATCH).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

try:  # delta-spark é opcional: sem ele, só os testes de integração com Delta são pulados
    from delta import configure_spark_with_delta_pip

    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False

# No Windows, escrever em disco local com Spark exige winutils (HADOOP_HOME). Sem isso,
# os testes de integração com Delta rodam só no CI (Linux).
if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
    HAS_DELTA = False


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("anp-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse.as_posix())
    )
    if HAS_DELTA:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def require_delta():
    if not HAS_DELTA:
        pytest.skip("delta-spark não instalado")
