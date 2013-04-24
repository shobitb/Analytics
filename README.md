Realtime and Big Data Analytics
=========
CSCI-GA.3033-008
NYU Courant Institute of Mathematical Sciences
Computer Science Department, Graduate Division
Spring 2013

Technologies
-----------
1. We use Apache Hadoop/MapReduce to analyze data sets to extract patterns within them.
2. We use Apache HBase to store data that we will later query from our UI.
3. We use Apache Mahout to cluster users using K-Means Algorithm (with SequenceFiles and NamedVectors representing User -> Weights of Tags of Songs Listened)
4. We use HDFS for keeping the large amounts of data which is not possible with running Hadoop in local/standalone mode.

Analytics 
---------
We try to answer the following from the Million Song Dataset:

1. What's the most listened-to song?
2. Who's the most listened-to artist?
3. What's an artist's top songs?
4. Plot a graph of the artist's song energies (0 - 100) vs. number of songs.
5. What makes a song famous (after creating clusters of users based on their listens using and song metadata)
