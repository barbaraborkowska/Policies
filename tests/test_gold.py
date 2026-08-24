from pyspark.sql import Row

test_data = [
    Row(
        incident_type="Collision",
        incident_severity="Major",
        fraud_reported="Y",
        total_claim_amount=1000.0
    ),
    Row(
        incident_type="Collision",
        incident_severity="Major",
        fraud_reported="N",
        total_claim_amount=2000.0
    ),
    Row(
        incident_type="Collision",
        incident_severity="Major",
        fraud_reported="Y",
        total_claim_amount=3000.0
    ),
    Row(
        incident_type="Theft",
        incident_severity="Minor",
        fraud_reported="N",
        total_claim_amount=500.0
    )
]

silver_df = spark.createDataFrame(test_data)

from pyspark.sql.functions import (
    col, count, sum, when, avg, round
)

gold_fraud_df = (
    silver_df
    .groupBy(
        "incident_type",
        "incident_severity"
    )
    .agg(
        count("*").alias("total_claims"),

        sum(
            when(col("fraud_reported") == "Y", 1).otherwise(0)
        ).alias("fraudulent_claims"),

        round(avg("total_claim_amount"), 2)
            .alias("avg_claim_amount"),

        round(sum("total_claim_amount"), 2)
            .alias("total_claim_amount")
    )
    .withColumn(
        "fraud_rate",
        round(
            col("fraudulent_claims") /
            col("total_claims") * 100,
            2
        )
    )
)

gold_fraud_df.show()

collision = (
    gold_fraud_df
    .filter(
        (col("incident_type") == "Collision") &
        (col("incident_severity") == "Major")
    )
    .collect()[0]
)

theft = (
    gold_fraud_df
    .filter(
        (col("incident_type") == "Theft") &
        (col("incident_severity") == "Minor")
    )
    .collect()[0]
)

# Collision / Major
assert collision["total_claims"] == 3
assert collision["fraudulent_claims"] == 2
assert collision["avg_claim_amount"] == 2000.0
assert collision["total_claim_amount"] == 6000.0
assert collision["fraud_rate"] == 66.67

# Theft / Minor
assert theft["total_claims"] == 1
assert theft["fraudulent_claims"] == 0
assert theft["avg_claim_amount"] == 500.0
assert theft["total_claim_amount"] == 500.0
assert theft["fraud_rate"] == 0.0

print("Gold fraud aggregation tests passed!")