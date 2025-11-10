/* קובץ מיגרציה ראשון:
  מגדיר את כל הטבלאות הבסיסיות של המערכת.
*/

-- 1. טבלת משתמשים
CREATE TABLE users (
    phone_number VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. טבלת החופים
CREATE TABLE beaches (
    slug VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE
);

-- 3. Favorites Table
CREATE TABLE favorites (
    phone_number VARCHAR(20) REFERENCES users(phone_number),
    beach_slug VARCHAR(100) REFERENCES beaches(slug),
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (phone_number, beach_slug)
);