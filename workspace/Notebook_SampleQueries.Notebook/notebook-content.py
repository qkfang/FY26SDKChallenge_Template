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
# Cell 1 – Verify tables are available
# ─────────────────────────────────────────────
tables = ["productcategory", "product", "customer", "salesorderheader", "salesorderdetail"]
for t in tables:
    n = spark.table(t).count()
    print(f"  {t}: {n} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 2 – Total Revenue by Product Category
# ─────────────────────────────────────────────
df_revenue_by_category = spark.sql("""
    SELECT
        pc.Name                                 AS Category,
        COUNT(DISTINCT soh.SalesOrderID)        AS TotalOrders,
        SUM(sod.OrderQty)                       AS UnitsSold,
        ROUND(SUM(sod.LineTotal), 2)            AS TotalRevenue
    FROM productcategory pc
    JOIN product         p   ON pc.ProductCategoryID = p.ProductCategoryID
    JOIN salesorderdetail sod ON p.ProductID          = sod.ProductID
    JOIN salesorderheader soh ON sod.SalesOrderID     = soh.SalesOrderID
    GROUP BY pc.Name
    ORDER BY TotalRevenue DESC
""")

print("=== Total Revenue by Product Category ===")
df_revenue_by_category.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 3 – Monthly Sales Trend (all periods)
# ─────────────────────────────────────────────
df_monthly = spark.sql("""
    SELECT
        YEAR(OrderDate)                     AS OrderYear,
        MONTH(OrderDate)                    AS OrderMonth,
        DATE_FORMAT(OrderDate, 'yyyy-MM')   AS YearMonth,
        COUNT(SalesOrderID)                 AS TotalOrders,
        ROUND(SUM(SubTotal), 2)             AS SubTotal,
        ROUND(SUM(TaxAmt),   2)             AS TaxAmount,
        ROUND(SUM(TotalDue), 2)             AS TotalRevenue
    FROM salesorderheader
    GROUP BY YEAR(OrderDate), MONTH(OrderDate), DATE_FORMAT(OrderDate, 'yyyy-MM')
    ORDER BY OrderYear, OrderMonth
""")

print("=== Monthly Sales Trend ===")
df_monthly.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 4 – Top 10 Customers by Total Revenue
# ─────────────────────────────────────────────
df_top_customers = spark.sql("""
    SELECT
        c.CustomerID,
        CONCAT(c.FirstName, ' ', c.LastName)    AS CustomerName,
        c.CompanyName,
        c.City,
        c.StateProvince,
        COUNT(DISTINCT soh.SalesOrderID)        AS TotalOrders,
        ROUND(SUM(soh.TotalDue), 2)             AS TotalRevenue
    FROM customer            c
    JOIN salesorderheader    soh ON c.CustomerID = soh.CustomerID
    GROUP BY c.CustomerID, c.FirstName, c.LastName, c.CompanyName, c.City, c.StateProvince
    ORDER BY TotalRevenue DESC
    LIMIT 10
""")

print("=== Top 10 Customers by Revenue ===")
df_top_customers.show(truncate=False)

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
        p.Name                              AS ProductName,
        p.ProductNumber,
        p.Color,
        p.ListPrice,
        SUM(sod.OrderQty)                   AS TotalUnitsSold,
        ROUND(SUM(sod.LineTotal), 2)        AS TotalRevenue
    FROM product             p
    JOIN salesorderdetail    sod ON p.ProductID = sod.ProductID
    GROUP BY p.ProductID, p.Name, p.ProductNumber, p.Color, p.ListPrice
    ORDER BY TotalUnitsSold DESC
    LIMIT 10
""")

print("=== Top 10 Best-Selling Products ===")
df_best_sellers.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 6 – Revenue and Orders by Sales Territory
# ─────────────────────────────────────────────
df_territory = spark.sql("""
    SELECT
        COALESCE(SalesTerritory, 'Unknown')     AS Territory,
        COUNT(SalesOrderID)                     AS TotalOrders,
        COUNT(DISTINCT CustomerID)              AS UniqueCustomers,
        ROUND(SUM(SubTotal),  2)                AS SubTotal,
        ROUND(SUM(TotalDue),  2)                AS TotalRevenue,
        ROUND(AVG(TotalDue),  2)                AS AvgOrderValue
    FROM salesorderheader
    GROUP BY SalesTerritory
    ORDER BY TotalRevenue DESC
""")

print("=== Revenue by Sales Territory ===")
df_territory.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 7 – Full Order Detail (order → product → category)
# ─────────────────────────────────────────────
df_order_detail = spark.sql("""
    SELECT
        soh.SalesOrderID,
        soh.OrderDate,
        CONCAT(c.FirstName, ' ', c.LastName)    AS CustomerName,
        c.CompanyName,
        soh.SalesTerritory,
        p.Name                                  AS ProductName,
        pc.Name                                 AS Category,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        ROUND(sod.LineTotal, 2)                 AS LineTotal,
        ROUND(soh.TotalDue, 2)                  AS OrderTotal
    FROM salesorderheader   soh
    JOIN customer           c   ON soh.CustomerID    = c.CustomerID
    JOIN salesorderdetail   sod ON soh.SalesOrderID  = sod.SalesOrderID
    JOIN product            p   ON sod.ProductID     = p.ProductID
    JOIN productcategory    pc  ON p.ProductCategoryID = pc.ProductCategoryID
    ORDER BY soh.OrderDate DESC, soh.SalesOrderID, sod.SalesOrderDetailID
""")

print(f"=== Full Order Detail ({df_order_detail.count()} line items) ===")
df_order_detail.show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 8 – Customer Segmentation by Lifetime Value
# ─────────────────────────────────────────────
df_segments = spark.sql("""
    WITH CustomerLTV AS (
        SELECT
            c.CustomerID,
            CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
            c.CompanyName,
            ROUND(SUM(soh.TotalDue), 2)          AS LifetimeValue,
            COUNT(DISTINCT soh.SalesOrderID)     AS OrderCount
        FROM customer         c
        JOIN salesorderheader soh ON c.CustomerID = soh.CustomerID
        GROUP BY c.CustomerID, c.FirstName, c.LastName, c.CompanyName
    )
    SELECT
        CASE
            WHEN LifetimeValue >= 10000 THEN 'High Value'
            WHEN LifetimeValue >=  5000 THEN 'Mid Value'
            ELSE 'Entry Level'
        END                          AS Segment,
        COUNT(CustomerID)            AS CustomerCount,
        ROUND(AVG(LifetimeValue), 2) AS AvgLifetimeValue,
        ROUND(SUM(LifetimeValue), 2) AS SegmentRevenue
    FROM CustomerLTV
    GROUP BY
        CASE
            WHEN LifetimeValue >= 10000 THEN 'High Value'
            WHEN LifetimeValue >=  5000 THEN 'Mid Value'
            ELSE 'Entry Level'
        END
    ORDER BY SegmentRevenue DESC
""")

print("=== Customer Segment Summary ===")
df_segments.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 9 – Year-over-Year Revenue Comparison
# ─────────────────────────────────────────────
df_yoy = spark.sql("""
    SELECT
        YEAR(OrderDate)             AS OrderYear,
        ROUND(SUM(TotalDue), 2)    AS TotalRevenue,
        COUNT(SalesOrderID)        AS TotalOrders,
        ROUND(AVG(TotalDue), 2)    AS AvgOrderValue
    FROM salesorderheader
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear
""")

print("=== Year-over-Year Revenue ===")
df_yoy.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────
# Cell 10 – Products with No Recent Orders (last 6 months)
# ─────────────────────────────────────────────
df_last_ordered = spark.sql("""
    SELECT
        p.ProductID,
        p.Name                              AS ProductName,
        p.ProductNumber,
        p.ListPrice,
        pc.Name                             AS Category,
        MAX(soh.OrderDate)                  AS LastOrderDate
    FROM product             p
    JOIN productcategory     pc  ON p.ProductCategoryID = pc.ProductCategoryID
    LEFT JOIN salesorderdetail sod ON p.ProductID       = sod.ProductID
    LEFT JOIN salesorderheader soh ON sod.SalesOrderID  = soh.SalesOrderID
    GROUP BY p.ProductID, p.Name, p.ProductNumber, p.ListPrice, pc.Name
    HAVING MAX(soh.OrderDate) IS NULL
        OR MAX(soh.OrderDate) < ADD_MONTHS(CURRENT_DATE(), -6)
    ORDER BY p.ListPrice DESC
""")

print(f"=== Products with No Orders in Last 6 Months ({df_last_ordered.count()} products) ===")
df_last_ordered.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
