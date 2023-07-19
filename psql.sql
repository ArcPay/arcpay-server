-- psql -U dev -d arcpay -f file.sql

CREATE TABLE user_balance (
    owner NUMERIC(49,0) NOT NULL, -- 20 bytes
    balance NUMERIC(13,0) NOT NULL, -- 5 bytes
    PRIMARY KEY (owner)
);
