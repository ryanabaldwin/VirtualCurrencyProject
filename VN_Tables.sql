CREATE DATABASE Dining;

CREATE TABLE VN_Transactions (
	id INT AUTO_INCREMENT PRIMARY KEY,
    external_user_id VARCHAR(50),
    loyalty_user_id VARCHAR(10),
    user_email VARCHAR(100),
    original_amount DECIMAL(10,2),
    altered_amount DECIMAL(10,2),
    type VARCHAR(50),
    comment VARCHAR(300),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    transactions_id VARCHAR(10),
    transactions_uuid VARCHAR(60),
    organization_name VARCHAR(30),
    instance VARCHAR(50),
    expired_at TIMESTAMP
);

CREATE TABLE VN_Credit (
	VC_key INT PRIMARY KEY,
    original_VC_amount DECIMAL(10,2),
    altered_VC_amount DECIMAL(10,2),
    phone_number VARCHAR(50),
    date_to_post TIMESTAMP,
    date_received TIMESTAMP,
    issuer_name VARCHAR(50),
    issuer_net_ID VARCHAR(20),
    issuer_email VARCHAR(100),
    sync_date TIMESTAMP,
    department_number INT,
    department VARCHAR(60),
    description VARCHAR(200),
    customer_name VARCHAR(50),
    posted VARCHAR(10),
    expiration_date TIMESTAMP
);

SELECT * FROM VN_Credit;

SELECT * FROM VN_Transactions;
