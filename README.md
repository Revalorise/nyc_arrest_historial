## NYC Arrests (2006 - Dec 2023) | STATUS: `UNFINISHED`
- This project was solely inspired by my wish to live & work in NYC, so I decided to analyze its crime rate in numerous categories including age, race, and gender.
- This project was also motivated by the idea of creating some "Data Engineering" project using pure Java to demonstrate my expertise in the language itself and my personal interests in NYC.
- This project is BY NO MEANS to accuse any age, race, or gender of being violent.
---
## Project Structure
![image](https://github.com/user-attachments/assets/42fd8479-a7df-46b2-9cc0-dcb8d2166831)
1. The project starts by assuming that we already have compressed(zip) file available on Google Cloud Storage.
2. Next, we use one of Java's class to extract that zip and get the csv file out of it.
3. Then, we create a database table, and insert the csv file into our newly created table.
4. And then, we use Apache Spark to clean up the data by dropping unused columns, and renaming values. Apply those changes directly into our table.
5. Next, we use Apache Spark to query data to satisfy our specific interest such as "The amount of crime by race and gender".
6. Lastly, we create a new database tables out of those query and visualize those newly created table using Power BI.
---
## Dashboard & Visualization
![image](https://github.com/user-attachments/assets/0307d6d2-e1be-4b55-ade9-b45d4c842160)
![image](https://github.com/user-attachments/assets/73420b5f-8254-4da9-97b1-22f25c3d2958)
