# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "49f3e9cc-39cc-49b9-8807-0d33674b04b5",
# META       "default_lakehouse_name": "SalesLakehouse",
# META       "default_lakehouse_workspace_id": "334687d9-c5a3-4af6-a9b1-9c02bad79934"
# META     }
# META   }
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 1 – Verify SalesLakehouse tables are available
# ─────────────────────────────────────────────
tables = spark.catalog.listTables()
print("Tables in SalesLakehouse:")
for t in tables:
    print(f"  {t.name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 2 – Top 10 Customers by Total Revenue
# ─────────────────────────────────────────────
df_top_customers = spark.sql("""
    SELECT
        c.CustomerID,
        CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
        c.CompanyName,
        COUNT(DISTINCT soh.SalesOrderID)     AS TotalOrders,
        ROUND(SUM(soh.SubTotal), 2)          AS TotalRevenue
    FROM customer c
    JOIN salesorderheader soh ON c.CustomerID = soh.CustomerID
    GROUP BY c.CustomerID, c.FirstName, c.LastName, c.CompanyName
    ORDER BY TotalRevenue DESC
    LIMIT 10
""")
print("=== Top 10 Customers by Revenue ===")
display(df_top_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 3 – Monthly Sales Trend
# ─────────────────────────────────────────────
df_monthly = spark.sql("""
    SELECT
        YEAR(OrderDate)           AS OrderYear,
        MONTH(OrderDate)          AS OrderMonth,
        COUNT(*)                  AS TotalOrders,
        ROUND(SUM(SubTotal), 2)   AS SubTotal,
        ROUND(SUM(TaxAmt), 2)     AS TaxAmount,
        ROUND(SUM(TotalDue), 2)   AS TotalRevenue
    FROM salesorderheader
    GROUP BY YEAR(OrderDate), MONTH(OrderDate)
    ORDER BY OrderYear, OrderMonth
""")
print("=== Monthly Sales Trend ===")
display(df_monthly)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 4 – Revenue by Product Category
# ─────────────────────────────────────────────
df_categories = spark.sql("""
    SELECT
        pc.Name                          AS Category,
        COUNT(DISTINCT sod.SalesOrderID) AS TotalOrders,
        SUM(sod.OrderQty)                AS UnitsSold,
        ROUND(SUM(sod.LineTotal), 2)     AS CategoryRevenue
    FROM productcategory pc
    JOIN product p          ON pc.ProductCategoryID = p.ProductCategoryID
    JOIN salesorderdetail sod ON p.ProductID        = sod.ProductID
    GROUP BY pc.Name
    ORDER BY CategoryRevenue DESC
""")
print("=== Revenue by Product Category ===")
display(df_categories)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 5 – Top 10 Best-Selling Products
# ─────────────────────────────────────────────
df_best_sellers = spark.sql("""
    SELECT
        p.ProductID,
        p.Name            AS ProductName,
        p.ProductNumber,
        p.Color,
        p.ListPrice,
        SUM(sod.OrderQty)            AS TotalUnitsSold,
        ROUND(SUM(sod.LineTotal), 2) AS TotalRevenue
    FROM product p
    JOIN salesorderdetail sod ON p.ProductID = sod.ProductID
    GROUP BY p.ProductID, p.Name, p.ProductNumber, p.Color, p.ListPrice
    ORDER BY TotalUnitsSold DESC
    LIMIT 10
""")
print("=== Top 10 Best-Selling Products ===")
display(df_best_sellers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 6 – Revenue by Sales Territory
# ─────────────────────────────────────────────
df_territory = spark.sql("""
    SELECT
        SalesTerritory,
        COUNT(*)                  AS TotalOrders,
        ROUND(SUM(SubTotal), 2)   AS SubTotal,
        ROUND(SUM(TotalDue), 2)   AS TotalRevenue
    FROM salesorderheader
    WHERE SalesTerritory IS NOT NULL
    GROUP BY SalesTerritory
    ORDER BY TotalRevenue DESC
""")
print("=== Revenue by Sales Territory ===")
display(df_territory)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 7 – Average Order Value by Customer Segment
# ─────────────────────────────────────────────
df_segment_base = spark.sql("""
    SELECT
        CustomerID,
        ROUND(SUM(TotalDue), 2)   AS CustomerRevenue,
        COUNT(*)                  AS OrderCount,
        ROUND(AVG(TotalDue), 2)   AS AvgOrderValue
    FROM salesorderheader
    GROUP BY CustomerID
""")
df_segment_base.createOrReplaceTempView("customer_summary")

df_segment = spark.sql("""
    SELECT
        CASE
            WHEN CustomerRevenue >= 10000 THEN 'High Value'
            WHEN CustomerRevenue >= 3000  THEN 'Mid Value'
            ELSE 'Entry Level'
        END                       AS CustomerSegment,
        COUNT(*)                  AS CustomerCount,
        ROUND(AVG(AvgOrderValue), 2) AS AvgOrderValue,
        ROUND(SUM(CustomerRevenue), 2) AS SegmentRevenue
    FROM customer_summary
    GROUP BY
        CASE
            WHEN CustomerRevenue >= 10000 THEN 'High Value'
            WHEN CustomerRevenue >= 3000  THEN 'Mid Value'
            ELSE 'Entry Level'
        END
    ORDER BY SegmentRevenue DESC
""")
print("=== Customer Segment Summary ===")
display(df_segment)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 8 – Products with No Recent Orders (last 12 months)
# ─────────────────────────────────────────────
from pyspark.sql.functions import col, max as spark_max, lit
from pyspark.sql import functions as F

df_order_dates = (
    spark.table("salesorderdetail").alias("sod")
    .join(spark.table("salesorderheader").alias("soh"), "SalesOrderID")
    .groupBy("ProductID")
    .agg(spark_max("OrderDate").alias("LastOrderDate"))
)

cutoff = "2024-03-07"  # 12 months before today (adjust as needed)

df_stale = (
    spark.table("product").alias("p")
    .join(df_order_dates.alias("lo"), col("p.ProductID") == col("lo.ProductID"), "left")
    .filter(col("lo.LastOrderDate").isNull() | (col("lo.LastOrderDate") < lit(cutoff)))
    .select(
        col("p.ProductID"),
        col("p.Name").alias("ProductName"),
        col("p.ProductNumber"),
        col("p.ListPrice"),
        col("lo.LastOrderDate"),
    )
    .orderBy(col("p.ListPrice").desc())
)
print(f"=== Products with No Orders since {cutoff} ({df_stale.count()} rows) ===")
display(df_stale)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
