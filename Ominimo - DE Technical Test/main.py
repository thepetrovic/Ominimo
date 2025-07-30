import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit, when, array, struct
import os

def load_metadata(path="metadata/motor_policy_metadata.json"):
    with open(path) as f:
        return json.load(f)

def main():
    spark = SparkSession.builder.appName("MotorPolicyIngestion").config("spark.hadoop.io.nativeio.enabled", "false").getOrCreate()
    metadata = load_metadata()
    flow = metadata["dataflows"][0]
    input_path = flow["sources"][0]["path"]

    df = spark.read.json(input_path)

    validations = flow["transformations"][0]["params"]["validations"]

    def create_validation_col(df, validations):
        conds = []
        for v in validations:
            field = v["field"]
            for rule in v["validations"]:
                if rule == "notEmpty":
                    cond = when((col(field).isNull()) | (col(field) == ""), lit(f"{field} is empty"))
                elif rule == "notNull":
                    cond = when(col(field).isNull(), lit(f"{field} is null"))
                else:
                    continue
                conds.append(cond)
        if conds:
            error_col = array(*[c for c in conds])
            df = df.withColumn("validation_errors", error_col)
            df = df.withColumn("validation_errors",
                expr("filter(validation_errors, x -> x is not null)")
            )
        else:
            df = df.withColumn("validation_errors", lit([]))
        return df

    from pyspark.sql.functions import expr
    df = create_validation_col(df, validations)

    ok_df = df.filter(expr("size(validation_errors) == 0")).drop("validation_errors")
    ko_df = df.filter(expr("size(validation_errors) > 0"))

    ok_df = ok_df.withColumn("ingestion_dt", current_timestamp())
    ko_df = ko_df.withColumn("ingestion_dt", current_timestamp())

    ok_path = flow["sinks"][0]["paths"][0]
    ko_path = flow["sinks"][1]["paths"][0]

    ok_df.write.mode("overwrite").json(ok_path)
    ko_df.write.mode("overwrite").json(ko_path)

    print("Processing completed.")

if __name__ == "__main__":
    main()
