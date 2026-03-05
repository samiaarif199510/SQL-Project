import pandas as pd
import sqlite3
from faker import Faker
import random
from datetime import datetime, date, timedelta

# registering adapters and convertering for date handling
sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_converter("DATE", lambda s: date.fromisoformat(s.decode()))

fake = Faker()

conn = sqlite3.connect("library_management.db", detect_types=sqlite3.PARSE_DECLTYPES)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")  # enforce foreign keys

# Create Table 1

# Student table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Students (
    student_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender TEXT CHECK(gender IN ('Male', 'Female', 'Other')),
    year_of_study INTEGER CHECK(year_of_study BETWEEN 1 AND 4),
    date_of_birth DATE,
    email TEXT UNIQUE,
    membership_type TEXT CHECK(membership_type IN ('Undergraduate','Postgraduate','PhD'))
);
""")

# Create Table 2

# Author table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Authors (
    author_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    nationality TEXT
);
""")

# Create Table 3

# Book table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Books (
    book_id INTEGER PRIMARY KEY,
    title TEXT,
    genre TEXT CHECK(genre IN ('Science','Engineering','Law','Arts','Medicine','Other')),
    publication_year INTEGER,
    isbn TEXT UNIQUE,
    pages INTEGER,
    author_id INTEGER,
    FOREIGN KEY(author_id) REFERENCES Authors(author_id)
);
""")

# Create Table 4

# Library Branches table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Library_Branches (
    branch_id INTEGER PRIMARY KEY,
    branch_name TEXT,
    city TEXT,
    capacity INTEGER
);
""")

# Create Table 5

# Book Copies table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Book_Copies (
    copy_id INTEGER PRIMARY KEY,
    book_id INTEGER,
    branch_id INTEGER,
    condition TEXT CHECK(condition IN ('New','Good','Fair','Poor')),
    available BOOLEAN,
    FOREIGN KEY(book_id) REFERENCES Books(book_id),
    FOREIGN KEY(branch_id) REFERENCES Library_Branches(branch_id)
);
""")

# Create Table 6

# Loan table with composite key
cursor.execute("""
CREATE TABLE IF NOT EXISTS Loans (
    student_id INTEGER,
    copy_id INTEGER,
    loan_date DATE,
    due_date DATE,
    return_date DATE,
    fine_amount REAL,
    PRIMARY KEY(student_id, copy_id, loan_date),
    FOREIGN KEY(student_id) REFERENCES Students(student_id),
    FOREIGN KEY(copy_id) REFERENCES Book_Copies(copy_id)
);
""")

conn.commit()

# Populate Students Table
# INTENTIONAL DUPLICATES: Include 15 duplicate emails to demonstrate UNIQUE constraint handling
# This tests data validation and constraint enforcement in production scenarios

genders = ['Male','Female','Other']
membership_types = ['Undergraduate','Postgraduate','PhD']

# Pre-generate a small pool of emails to reuse for intentional duplicates
duplicate_emails = [fake.email() for _ in range(5)]

for i in range(1000 + 15):  # 1000 normal students + 15 with duplicate emails
    first_name = fake.first_name()
    last_name = fake.last_name()
    gender = random.choice(genders)
    year_of_study = random.randint(1,4)
    dob = fake.date_of_birth(minimum_age=18, maximum_age=30)
    
    # Force duplicate emails for first 15 inserts to demonstrate constraint testing
    if i < 15:
        email = random.choice(duplicate_emails)
    else:
        email = fake.email()
    
    membership = random.choice(membership_types)
    # Insert Query    
    try:
        cursor.execute("""
        INSERT INTO Students (first_name,last_name,gender,year_of_study,date_of_birth,email,membership_type)
        VALUES (?,?,?,?,?,?,?)
        """, (first_name,last_name,gender,year_of_study,dob,email,membership))
    except sqlite3.IntegrityError:
        # duplicate email encountered, skip this record (expected for constraint testing)
        continue

conn.commit()

# Populate Authors Table

for _ in range(200):  # 200 authors
    first_name = fake.first_name()
    last_name = fake.last_name()
    nationality = fake.country()
    # Insert Query    
    cursor.execute("""
    INSERT INTO Authors (first_name,last_name,nationality) VALUES (?,?,?)
    """, (first_name,last_name,nationality))

conn.commit()

# Populate Books Table
# INTENTIONAL DUPLICATES: Include 8 duplicate ISBNs to demonstrate UNIQUE constraint handling
# This tests ISBN uniqueness validation similar to real library data migration scenarios

author_ids = [row[0] for row in cursor.execute("SELECT author_id FROM Authors").fetchall()]
genres = ['Science','Engineering','Law','Arts','Medicine','Other']

# Pre-generate a small pool of ISBNs to reuse for intentional duplicates
fixed_isbns = [fake.isbn13() for _ in range(5)]

for i in range(500 + 8):  # 500 normal books + 8 with duplicate ISBNs
    title = fake.sentence(nb_words=4)
    genre = random.choice(genres)
    pub_year = random.randint(1980,2023)
    
    # Force duplicate ISBNs for first 8 inserts to demonstrate constraint testing
    if i < 8:
        isbn = random.choice(fixed_isbns)
    else:
        isbn = fake.isbn13()
    
    pages = random.randint(50,1000)
    author_id = random.choice(author_ids)
    # Insert Query
    try:
        cursor.execute("""
        INSERT INTO Books (title,genre,publication_year,isbn,pages,author_id) VALUES (?,?,?,?,?,?)
        """, (title,genre,pub_year,isbn,pages,author_id))
    except sqlite3.IntegrityError:
        # duplicate ISBN, skip this record (expected for constraint testing)
        continue

conn.commit()

# Populate Library Branches

branches = [
    ('Central Library','City A',5000),
    ('Science Library','City B',3000),
    ('Engineering Library','City C',2500)
]
# Insert Query
cursor.executemany("INSERT INTO Library_Branches (branch_name,city,capacity) VALUES (?,?,?)", branches)
conn.commit()

# Populate Book Copies

book_ids = [row[0] for row in cursor.execute("SELECT book_id FROM Books").fetchall()]
branch_ids = [row[0] for row in cursor.execute("SELECT branch_id FROM Library_Branches").fetchall()]
conditions = ['New','Good','Fair','Poor']

for copy_id in range(1,1500):  # 1500 copies
    book_id = random.choice(book_ids)
    branch_id = random.choice(branch_ids)
    condition = random.choices(conditions,weights=[50,30,15,5])[0]
    available = random.choice([True,False])
    # Insert Query
    cursor.execute("""
    INSERT INTO Book_Copies (book_id,branch_id,condition,available) VALUES (?,?,?,?)
    """, (book_id,branch_id,condition,available))

conn.commit()

# Populate Loans Table

student_ids = [row[0] for row in cursor.execute("SELECT student_id FROM Students").fetchall()]
copy_ids = [row[0] for row in cursor.execute("SELECT copy_id FROM Book_Copies").fetchall()]

for _ in range(2000):  # 2000 loans
    student_id = random.choice(student_ids)
    copy_id = random.choice(copy_ids)
    loan_date = fake.date_between(start_date='-2y',end_date='today')
    due_date = loan_date + timedelta(days=14)
    return_date = loan_date + timedelta(days=random.randint(0,28)) if random.random()>0.2 else None
    fine_amount = round(max(0,(return_date - due_date).days * 1.0),2) if return_date else 0.0
    # Insert Query
    try:
        cursor.execute("""
        INSERT INTO Loans (student_id,copy_id,loan_date,due_date,return_date,fine_amount)
        VALUES (?,?,?,?,?,?)
        """,(student_id,copy_id,loan_date,due_date,return_date,fine_amount))
    except sqlite3.IntegrityError:
        continue  # skip duplicates for composite key

conn.commit()

# Summary

print("Database created successfully!")
print("Total Students:", cursor.execute("SELECT COUNT(*) FROM Students").fetchone()[0])
print("Total Authors:", cursor.execute("SELECT COUNT(*) FROM Authors").fetchone()[0])
print("Total Books:", cursor.execute("SELECT COUNT(*) FROM Books").fetchone()[0])
print("Total Book Copies:", cursor.execute("SELECT COUNT(*) FROM Book_Copies").fetchone()[0])
print("Total Loans:", cursor.execute("SELECT COUNT(*) FROM Loans").fetchone()[0])

# Export tables to CSV

tables = ["Students", "Authors", "Books", "Library_Branches", "Book_Copies", "Loans"]

for table in tables:
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df.to_csv(f"{table}.csv", index=False)

print("CSV files exported successfully!")

conn.close()
