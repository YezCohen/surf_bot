-- Migration Script: add subscriptions to users table

ALTER TABLE users
ADD COLUMN subscription_status VARCHAR(20) DEFAULT 'unsubscribed' NOT NULL,
ADD COLUMN subscription_frequency VARCHAR(10) DEFAULT 'weekly' NOT NULL;
