# Skillability HarvardX Data Science Capstone
We analyse the [Stack Overflow](https://stackoverflow.com/) data using multiple Data Science and Machine Learning techniques.

## The What: skills and technology trends
We use dimensionality reduction, more specifically the principal component analysis (PCA) to discover the skills or technologies explaining most of the variance in the Stack Overflow data i.e. the technology trends.

![Technology trends](https://raw.githubusercontent.com/bravegag/HarvardX-Skillability/master/images/pca.png "Technology trends")

## The Where: putting it in geographical context
We take those technology trends and put them in Geographical context for Switzerland.

![Switzerland](https://raw.githubusercontent.com/bravegag/HarvardX-Skillability/master/images/switzerland.png "Switzerland")

## The How: rating user skills
We employ Statistical Inference to build a new derived `ratings` dataset providing user skill ratings. Finally we build a recommender system using collaborative filtering (CF) and the low-rank matrix factorization model-based approach (LRMF) to predict user skill ratings.

# Project files

* The [`create_dataset.r`](https://raw.githubusercontent.com/bravegag/HarvardX-Skillability/master/create_dataset.r) script will automatically download, extract, parse and clean the Stack Overflow data files. It will also construct the new derived `ratings` dataset.
* The [`skillability.r`](https://raw.githubusercontent.com/bravegag/HarvardX-Skillability/master/skillability.r) script contains all the data science analysis code. A new LRMF model implementation is integrated with the popular `caret` machine learning library for calibration, training and prediction.
* The [`skillability.pdf`](https://raw.githubusercontent.com/bravegag/HarvardX-Skillability/master/skillability.pdf) contains the final project report.
