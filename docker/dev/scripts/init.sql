-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS ${zs_db};

-- Use the database
USE ${zs_db};

-- Add any initial tables, data, or configuration here
-- For example:
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add any seed data if needed
INSERT INTO example_table (name) VALUES
    ('Test Entry 1'),
    ('Test Entry 2');

-- Add any additional MySQL configuration
SET GLOBAL time_zone = '+00:00';