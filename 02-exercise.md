# 📚 NoSQL Mini-Project: Library Catalog with MongoDB

In this project, you’ll design a **Library Catalog** using **MongoDB**, a popular open-source NoSQL document database.  
The project simulates how real libraries store, search, and analyze data.  



## 🎯 Goal
By the end of this exercise, you should be able to:  
- Understand **NoSQL document structures** (flexible schemas, nested fields)  
- Load and explore data from an **open dataset**  
- Run **queries** (filters, ranges, nested fields)  
- Perform **aggregations** (counts, averages, group by)  
- Update documents dynamically  



## 📂 Dataset
Use an **open-source dataset** about books. Possible options:  
- [Open Library Dataset](https://openlibrary.org/developers/dumps) (JSON data about books and authors)  
- Any Kaggle dataset about books (e.g., Goodreads or book metadata)  

👉 Choose a dataset that contains at least:  
- Book titles  
- Authors  
- Publication year  
- Genres or subjects  
- Ratings or popularity information (if available)  



## 🛠️ Setup

1. Run MongoDB locally or with Docker:  
   ```bash
   docker run -d -p 27017:27017 --name mongodb mongo:6.0
   ```

2. Install the Python client:  
   ```bash
   pip install pymongo
   ```

3. Start a Python notebook or script to interact with the database.  



## 📋 General Guidelines

### Step 1. Design Your Document Model
Think about how to store your book data in MongoDB. Example structure:  
```json
{
  "title": "The Hobbit",
  "author": {"name": "J.R.R. Tolkien", "birth_year": 1892},
  "genres": ["Fantasy"],
  "year": 1937,
  "ratings": [5, 4, 5, 5]
}
```
- You can embed the **author** inside the book document  
- Use arrays for **genres** and **ratings**  

### Step 2. Insert Data
- Load your chosen dataset (CSV or JSON) with Python/pandas  
- Insert it into a MongoDB collection called `books`  
- Insert at least **100 records**  

### Step 3. Run Queries
Write queries such as:  
- Find all books in a given genre  
- Find books published after a certain year  
- Find books by a specific author  

### Step 4. Aggregations
Use MongoDB’s aggregation pipeline to answer:  
- What is the **average rating** per book?  
- How many books are there per genre?  
- Which authors are most common in the dataset?  

### Step 5. Updates
Practice modifying documents:  
- Add a new rating to a book  
- Add missing author fields if incomplete  
- Update genre lists  


## ✅ Deliverables

1 repository and your code and queries should follow **clean coding practices**:  
- Use clear variable names  
- Add docstrings to functions  
- Organize queries and aggregations in reusable functions  
- Avoid duplicating logic  


### 🟢 Must-Have
By the end, you should have:  
1. A `books` collection with a meaningful number of documents (at least ~100)  
2. Several **basic queries** exploring the data (e.g., filtering, nested fields, range queries)  
3. At least one **aggregation** to get insights from the data (e.g., average, count, group by)  
4. At least one **update** modifying a document  

### 🌟 Nice-to-Have
If you finish early or want to explore advanced features:  
- Add additional collections (e.g., `users` with borrowing history)  
- Run more complex queries across collections (e.g., *Which users borrowed Sci-Fi books?*)  
- Perform more advanced aggregations (e.g., *most borrowed authors*)  
- Build a simple dashboard or visualization with [MongoDB Compass](https://www.mongodb.com/products/compass) or another tool  
- Add **error handling and logging** in your Python code  
- Explore indexing strategies or text search features for improved performance  



💡 **Key takeaway:** MongoDB (and NoSQL in general) is flexible: you don’t need a fixed schema, you can embed nested data, and you can run powerful analytics directly in the database.