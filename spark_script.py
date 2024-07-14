import json
import re
from pyspark import SparkConf, SparkContext
import os
from operator import add
import time



def get_asin_brand(line):
    lineData   = json.loads(line)
    asin_data  = lineData.get("asin")
    brand_data = lineData.get("brand")
    return (str(asin_data), str(brand_data))


def get_review_rating(line):
    lineData        = json.loads(line)
    asin_data       = lineData.get("asin")
    reviewTime_data = lineData.get("reviewTime")
    overall_data    = lineData.get("overall")
    return ((str(asin_data), str(reviewTime_data)), float(overall_data))


def get_review_count(line):
    lineData        = json.loads(line)
    asin_data       = lineData.get("asin")
    reviewTime_data = lineData.get("reviewTime")
    return ((str(asin_data), str(reviewTime_data)), 1)


def review_rating_filter(line):
    return True if line[0][0]!='' and line[0][1]!='' and line[1]>0.0 else False


def concatenate_output(output_dir, output_file, prefix='part-'):
    os.chdir(output_dir)
    input_files = [file for file in os.listdir() if file.startswith(prefix)]
    with open(output_file, 'w') as output:
        for input_file in input_files:
            with open(input_file, 'r') as input:
                output.write(input.read())
    os.chdir('../')


def save_final_result(result_outfile, rdd):
    with open(result_outfile, 'w') as output:
        results = rdd.take(10)
        for result in results:
            output.write(f"{result}\n") 



review_filename = "Grocery_and_Gourmet_Food.json"
review_outputdir= 'review_rdd_output'
review_outfile  = 'review_rdd_outfile.txt'

meta_filename   = "meta_Grocery_and_Gourmet_Food.json"
meta_outputdir  = 'meta_rdd_output'
meta_outfile    = 'meta_rdd_outfile.txt'

join_outputdir  = 'join_output'
join_outfile    = 'join_rdd_outfile.txt'

result_outfile  = 'top10result.txt'


start = time.time()
conf = SparkConf()
sc = SparkContext(conf = conf)



##########################################################################################################
### Metadata integration: Create an RDD from the metadata file where the key = asin, and value = brand ###
##########################################################################################################
os.system(f"rm -rf {meta_outputdir}")                                        # Delete existing meta_rdd output directory to avoid Spark error on existing file with same name

meta_rdd          = sc.textFile(meta_filename)                               # Read the meta data and convert into RDD
meta_rdd          = meta_rdd.map(get_asin_brand)                             # Map meta data with new RDD containing just 'asin' and 'brand'
meta_rdd          = meta_rdd.filter(lambda line: line[1]!='')                # Filter the RDD with any record having empty 'brand'
meta_rdd.saveAsTextFile(meta_outputdir)                                      # Save in a folder
concatenate_output(meta_outputdir, meta_outfile)                             # Concatenating output files from all slave nodes

##########################################################################################################################
### Review Analysis: Create an RDD from review file where key = asin, and value = (average_rating, daily_review_count) ###
##########################################################################################################################
os.system(f"rm -rf {review_outputdir}")                                      # Delete existing review output directory to avoid Spark error on existing file with same name

review_rdd = sc.textFile(review_filename)                                    # Read the review data and convert into RDD
review_rdd = review_rdd.map(get_review_rating)                               # Map review data with new RDD containing just 'asin', 'reviewTime', and 'overall'
review_rdd = review_rdd.filter(review_rating_filter)                         # Filter the RDD with any record having empty 'asin', empty 'reviewTime', or unexpected 'overall'

review_rdd = review_rdd.map(lambda x: (x[0], (x[1], 1)))                     # Transformation: (key=('asin', 'reviewTime'), val='overall') ===>>> (key=('asin', 'reviewTime'), val=('overall', 1))
review_rdd = review_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) # ReduceByKey: For matching key=('asin', 'reviewTime'), sum 'overall' rating and review_count=1 per record respectively
review_rdd = review_rdd.map(lambda x: (x[0], (x[1][0] / x[1][1], x[1][1])))  # Transformation: (key=('asin', 'reviewTime'), val=('overall', 1)) ===>>> (key=('asin', 'reviewTime'), val=(averate_rating, daily_review_count))
review_rdd = review_rdd.sortBy(lambda x: x[1][1], ascending=False)           # Sort in descending order based on daily_review_count
review_rdd = review_rdd.map(lambda x: (x[0][0], (x[1][1], x[1][0], x[0][1])))# Transformation: (key=('asin', 'reviewTime'), val=(averate_rating, daily_review_count)) ===>>> (key='asin', val=(daily_review_count, average_rating, 'reviewTime'))
review_rdd.saveAsTextFile(review_outputdir)                                  # Save in a folder
concatenate_output(review_outputdir, review_outfile)                         # Concatenating output files from all slave nodes

##########################################################################
### Review Analysis: Merge meta_rdd and review_rdd based on key='asin' ###
##########################################################################
os.system(f"rm -rf {join_outputdir} {result_outfile}")                       # Delete existing review output directory to avoid Spark error on existing file with same name

join_rdd  = review_rdd.join(meta_rdd)                                        # Merge review_rdd and meta_rdd based on key='asin'
join_rdd  = join_rdd.map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1])))# Transformation: (key='asin', val=((daily_review_count, average_rating, 'reviewTime'), brand)) ===>>> (key='asin', val=(daily_review_count, average_rating, brand))
join_rdd  = join_rdd.sortBy(lambda x: x[1][0], ascending=False)              # Sort in descending order based on daily_review_count
join_rdd.saveAsTextFile(join_outputdir)                                      # Save in a folder
concatenate_output(join_outputdir, join_outfile)                             # Concatenating output files from all slave nodes

save_final_result(result_outfile, join_rdd)                                  # Save top 10 brand with highest daily review count



sc.stop()

end = time.time()
print(f"\n\n\nExecution Time: {end-start:.2f} seconds\n\n\n")
