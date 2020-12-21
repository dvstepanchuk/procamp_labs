# Hadoop MapReduce Top 5 Airlines Application

- Clone the repository
`git clone https://github.com/omklymenko/procamp_labs.git`
- Run `./upload_from_local_to_hdfs.sh`
(short list of the flights will be used. To work with full list download [source data](https://www.kaggle.com/usdot/flight-delays) and copy to hdfs)
- Run 
`./submit_top_5_airlines.sh`
- To find the results run
 `hadoop dfs -ls /procamp_labs/top_5_airlines/output/out2`
