from DataBase.base import session

query = """
CREATE OR REPLACE VIEW insights AS
SELECT county,
       COUNT(*) AS sales_count,
       SUM(CAST(price AS int)) AS sales_total,
       MAX(CAST(price AS int)) AS sales_max,
       MIN(CAST(price AS int)) AS sales_min,
       AVG(CAST(price AS int))::numeric(10,2) AS sales_avg
FROM ppr_clean_all
GROUP BY county
"""

def execute():
    session.execute(query)
    session.commit()