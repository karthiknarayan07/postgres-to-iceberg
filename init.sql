CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_date DATE,
    country VARCHAR(50),
    city VARCHAR(50),
    amount DECIMAL(10,2)
);

INSERT INTO orders (order_date, country, city, amount) VALUES
('2024-01-01', 'USA', 'New York', 120.50),
('2024-01-02', 'USA', 'Los Angeles', 99.99),
('2024-01-03', 'India', 'Mumbai', 200.00),
('2024-01-04', 'India', 'Delhi', 150.75),
('2024-01-05', 'Germany', 'Berlin', 300.40);
