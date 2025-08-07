from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

class DQValidator:
    def __init__(self, df: DataFrame):
        self.df = df

    def null_check(self, columns: list):
        return self.df.select([
            count(when(col(c).isNull(), c)).alias(f"{c}_nulls") for c in columns
        ])

    def duplicate_check(self, subset: list):
        return self.df.groupBy(subset).count().filter("count > 1")

    def range_check(self, column: str, min_val, max_val):
        return self.df.filter((col(column) < min_val) | (col(column) > max_val))

    def referential_integrity_check(self, child_df: DataFrame, join_col: str):
        return child_df.join(self.df, join_col, "left_anti")
