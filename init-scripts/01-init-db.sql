-- Create sample table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INT,
    city VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create another table for orders (demonstrating embedded documents)
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10, 2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email, age, city) VALUES
    ('John Doe', 'john@example.com', 30, 'New York'),
    ('Jane Smith', 'jane@example.com', 25, 'Los Angeles'),
    ('Bob Wilson', 'bob@example.com', 35, 'Chicago');

INSERT INTO orders (user_id, product_name, quantity, price) VALUES
    (1, 'Laptop', 1, 999.99),
    (1, 'Mouse', 2, 25.50),
    (2, 'Keyboard', 1, 75.00),
    (3, 'Monitor', 2, 299.99);

-- Create publication for CDC
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
