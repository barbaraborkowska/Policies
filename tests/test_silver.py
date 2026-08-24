from pyspark.sql.functions import col, when, lit, concat_ws
from pyspark.sql import Row

test_data = [
    Row(
        policy_number="P001",
        policy_bind_date="2020-01-01",
        incident_date="2020-06-01",
        total_claim_amount=1000,
        age=35,
        months_as_customer=24,
        policy_annual_premium=1200,
        number_of_vehicles_involved=1,
        incident_hour_of_the_day=14,
        fraud_reported="N",
        property_damage="NO",
        police_report_available="YES"
    )
]

silver_df = spark.createDataFrame(test_data)


validated_df = (
    silver_df
    .withColumn(
        "validation_error",
        concat_ws(
            "; ",
            when(col("policy_number").isNull(), lit("missing_policy_number")),
            when(col("policy_bind_date").isNull(), lit("missing_policy_bind_date")),
            when(col("incident_date").isNull(), lit("missing_incident_date")),
            when(col("total_claim_amount").isNull(), lit("missing_total_claim_amount")),

            when(
                (col("age") < 18) | (col("age") > 100),
                lit("invalid_age")
            ),

            when(
                col("months_as_customer") < 0,
                lit("invalid_customer_tenure")
            ),

            when(
                col("policy_annual_premium") < 0,
                lit("negative_premium")
            ),

            when(
                col("total_claim_amount") < 0,
                lit("negative_claim_amount")
            ),

            when(
                col("number_of_vehicles_involved") < 1,
                lit("invalid_vehicle_count")
            ),

            when(
                (col("incident_hour_of_the_day") < 0) |
                (col("incident_hour_of_the_day") > 23),
                lit("invalid_incident_hour")
            ),

            when(
                col("incident_date") < col("policy_bind_date"),
                lit("incident_before_policy")
            ),

            when(
                col("fraud_reported").isin("Y", "N") == False,
                lit("invalid_fraud_flag")
            ),

            when(
                col("property_damage").isin("YES", "NO") == False,
                lit("invalid_property_damage")
            ),

            when(
                col("police_report_available").isin("YES", "NO") == False,
                lit("invalid_police_report")
            )
        )
    )
)

result = validated_df.collect()[0]

assert result["validation_error"] == ""

