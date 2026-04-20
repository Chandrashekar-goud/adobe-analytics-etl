"""
Shared pytest fixtures.

SparkSession is scoped to the entire test session to avoid the large
overhead of spinning up a new JVM per test module.

Java compatibility note:
  PySpark 3.5.x supports Java 17/21. The --add-opens flags below
  are required for Java 17+ to permit Spark's internal JVM reflection
  calls that would otherwise be blocked by the module system.
"""

import pytest
from pyspark.sql import SparkSession

# Required --add-opens flags for Java 17+ compatibility with PySpark 3.5.x
# These grant Spark access to internal JVM APIs it needs for memory management
# and serialization. Without them, SparkContext fails on Java 17/21.
_JAVA_OPTIONS = " ".join([
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
])


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession for unit testing."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("UnitTests")
        .config("spark.sql.shuffle.partitions",          "2")
        .config("spark.default.parallelism",             "2")
        .config("spark.driver.extraJavaOptions",         _JAVA_OPTIONS)
        .config("spark.executor.extraJavaOptions",       _JAVA_OPTIONS)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()