# Introduction
This repo contains Python code examples to analyze and search Facebook graph data with the new [GraphFrames](http://graphframes.github.io/) capability in Spark.  The example shows an end-to-end flow for loading, running queries and graph algorithms.

[Consult this blog post on mapr.com](https://www.mapr.com/blog/using-spark-graphframes-analyze-facebook-connections) for more info.

MapR provides a fast, developer-friendly platform for Spark along with a sandbox VM that gives you an easy way to start trying some examples with your own data.

### Loading the Example onto MapR

Use the following steps to run these examples on the sandbox. 

* Download the latest MapR sandbox from [mapr.com/sandbox](http://mapr.com/sandbox). You can also install the MapR Converged Community Edition on a cluster for free at [mapr.com/download](http://mapr.com/download), or start with a pre-existing cluster already running Spark.  

* After logging in as the 'mapr' user on the sandbox, download the GraphFrames python package. It's also convenient to make a link to the python module to the same directory where you will be submitting the job:
```
wget https://github.com/graphframes/graphframes/archive/master.tar.gz
tar xvfz master.tar.gz
ln -s graphframes-master/python/graphframes ./graphframes
```
* Grab the [Stanford Facebook data set files](https://snap.stanford.edu/data/egonets-Facebook.html) and move them into MapR-FS:
```
wget https://snap.stanford.edu/data/facebook.tar.gz
wget https://snap.stanford.edu/data/facebook_combined.txt.gz
wget https://snap.stanford.edu/data/readme-Ego.txt
tar xvfz facebook.tar.gz -C /mapr/demo.mapr.com/user/mapr --strip 1
gunzip -c facebook_combined.txt.gz > /mapr/demo.mapr.com/user/mapr
```
* Download these code examples:
```
wget https://raw.githubusercontent.com/mapr-demos/spark-graphframes/master/gframes.py
```

### Running the Code with ```spark-submit```
All of the code is in the file ```gframes.py```.  After completing the above steps you can run the example as follows (you may need to adjust parts of this according to the Spark and/or graphframes versions):

```
/opt/mapr/spark/spark-1.5.2/bin/spark-submit --packages graphframes:graphframes:0.1.0-spark1.5,com.databricks:spark-csv_2.11:1.4.0 --master yarn --deploy-mode client gframes.py
```
The code will create several RDDs from the input files, build a unified DataFrame and GraphFrame of the 'friends' network, and output the results of motif queries, searches and PageRank.




