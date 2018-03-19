# Predicting US census Income

## Introduction : 
US Adult Census data relating income to social factors such as Age, Education, race etc. The Us Adult income dataset was extracted by Barry Becker from the 1994 US Census Database. The data set consists of anonymous information such as occupation, age, native country, race, capital gain, capital loss, education, work class and more. _Each row is labelled as either having a salary greater than ">50K" or "<=50K"._

- This Data set is split into two CSV files, named **adult-training.txt** and **adult-test.txt**
The goal here is to train a binary classifier on the training dataset to predict the column income_bracket which has two possible values ">50K" and "<=50K" and evaluate the accuracy of the classifier with the test dataset. 

- Note that the dataset is made up of categorical and continuous features. It also contains missing values The categorical columns are: workclass, education, marital_status, occupation, relationship, race, gender, native_country

The training data doesn’t have column names, so specifically adding it. Both datasets i.e. training, and testing are combined. After cleaning the data, we were left with 32561 rows with 15 columns. The dataset had NA’s in work class & occupation,
we removed the Na’s. The fnlwgt was removed, since it had no effect on target variable.

-	Here, there is no column name in given data, so we need to define column name first
-	Row in test data has many unknown variables so skipping the row. 
-	For better pre-processing combining the data & divided it into 80-20.
- For more information please visit [here](https://www.census.gov/)

## Get Dataset from 
 •	[Kaggle](https://www.kaggle.com/johnolafenwa/us-census-data/data) 
 •	[UCI](https://archive.ics.uci.edu/ml/datasets/US+Census+Data+(1990))

## IDE - 
  [Jupyter Notebook](http://jupyter.org/)

## Algorithms - 
 • [Linear Regression](https://en.wikipedia.org/wiki/Linear_regression)
 • [Linear Discriminant Analysis](https://en.wikipedia.org/wiki/Linear_discriminant_analysis)
 • [Logistic Regression](https://en.wikipedia.org/wiki/Logistic_regression)
 • [K Nearest Neigbor](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm)
 • [Decision Tree](https://en.wikipedia.org/wiki/Decision_tree_learning)
 • [Support Vector Machine](https://en.wikipedia.org/wiki/Support_vector_machine)
 • [Naive Bayes](https://en.wikipedia.org/wiki/Naive_Bayes_classifier)
 • [K-Means](https://en.wikipedia.org/wiki/K-means_clustering)
 • [Neural Network](https://en.wikipedia.org/wiki/Artificial_neural_network)
 • [XgBoost](https://en.wikipedia.org/wiki/Xgboost)
 
 ## Results 
 ![Results](https://user-images.githubusercontent.com/21111859/33803528-9ae0bc82-dd60-11e7-9563-4689561a37b5.png)
 
 XgBoost gave us the best accuracy. 
