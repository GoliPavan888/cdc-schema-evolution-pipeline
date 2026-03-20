from pathlib import Path

SQL_TEMPLATE = """USE inventory;

CREATE TEMPORARY TABLE digits (d INT PRIMARY KEY);
INSERT INTO digits (d) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

CREATE TEMPORARY TABLE seq_1m AS
SELECT (@rownum := @rownum + 1) AS n
FROM digits a
CROSS JOIN digits b
CROSS JOIN digits c
CROSS JOIN digits d
CROSS JOIN digits e
CROSS JOIN digits f
CROSS JOIN (SELECT @rownum := 0) r
LIMIT 600000;

INSERT INTO customers (first_name, last_name, email, created_at)
SELECT
    CONCAT('First', n),
    CONCAT('Last', n),
    CONCAT('customer', n, '@example.com'),
    TIMESTAMPADD(MINUTE, n, '2024-01-01 00:00:00')
FROM seq_1m
WHERE n <= {customers};

INSERT INTO products (name, description, price)
SELECT
    CONCAT('Product ', n),
    CONCAT('Description for product ', n),
    ROUND(5 + (n % 5000) / 10, 2)
FROM seq_1m
WHERE n <= {products};

INSERT INTO orders (customer_id, product_id, quantity, order_date)
SELECT
    ((n - 1) % {customers}) + 1,
    ((n - 1) % {products}) + 1,
    ((n - 1) % 5) + 1,
    TIMESTAMPADD(SECOND, n, '2024-02-01 00:00:00')
FROM seq_1m
WHERE n <= {orders};

DROP TEMPORARY TABLE IF EXISTS seq_1m;
DROP TEMPORARY TABLE IF EXISTS digits;
"""


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    output_path = root / "mysql" / "init" / "02-data.sql"

    sql = SQL_TEMPLATE.format(customers=100000, products=50000, orders=400000)
    output_path.write_text(sql, encoding="utf-8")
    print(f"Wrote seed SQL to {output_path}")


if __name__ == "__main__":
    main()
