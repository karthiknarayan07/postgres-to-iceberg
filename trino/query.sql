-- Connect: http://localhost:8080
SHOW TABLES FROM iceberg.default;

SELECT * FROM iceberg.default.orders;
SELECT * FROM iceberg.default.orders WHERE country='India' AND city='Delhi';
