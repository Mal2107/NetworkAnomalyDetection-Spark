SPARK STREAMING FOR ANAMOLY DETECTION 

Dataset Link = https://www.kaggle.com/anushonkar/network-anamoly-detection/

Dataset info
	Rows ~ 1.2 Lakh
	Columns  - 43

	
Each row contains information about the packet going to a end point.
It also contains a label that defines the type of attack that is being performed

Data needs to be cleaned. Some columns have all zero values
Data needs to be scaled as range of values for SRC and DEST bytes is very big

Major types of classes - DOS (Denial of service) ; Probe (Surveillance attacks) ; U2R (Unauthorized access to local machines root user) ; R2L (Unauthorized access from remote machine)

We first stream the data using spark streaming, train 3 classifiers 
Then we strem the training dataset and see that we are achieveing an accuracy of around 80% on two of the three classifiers (BERnoulliNB , MultinomialNB , Passive Agressive Classifier)

At the end we perfom clustering on the testing dataset using kMeans Clustering and showing setting number of clusters to 4 

