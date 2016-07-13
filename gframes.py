from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import Row
import re

sc = SparkContext()
sqlContext = SQLContext(sc)
peopleids = [0, 107, 1684, 1912, 3437, 348, 3980, 414, 686, 698]
featids = ["id", "birthday", "hometown_id", "work_employer_id",
    "education_school_id", "education_year_id" ]
formatter = 'com.databricks.spark.csv'
vtx = Row(*featids)

# load the entire edge and node set into a Spark DataFrame
edf = sqlContext.read.format(formatter).options(delimiter=' ', \
    header='false', inferSchema=True) \
    .load('facebook_combined.txt').withColumnRenamed( \
    'C0', 'src').withColumnRenamed('C1', 'dst')

def featurematch(a, b):
    return "%s != 'None' and %s != 'None' and %s = %s" % \
            (a, b, a, b)

def fn_process(line):
    psd = re.search(
        r'\d+ (.*);anonymized feature (\d+)', line, re.IGNORECASE)
    if not psd:
        print "parse error, line: %s" % line
        sys.exit(1)
    n = psd.group(1).replace(';', '_')
    #n = re.sub('_$', '', n)
    f = psd.group(2)
    return (n, f)

def feat_process(line, selfid):
    allents = line.split(' ')
    if (selfid != -1):
        return (selfid, allents)
    else:
        return (allents[0], allents[1:])

def get_feats(vtxid, mapping, feats):
    thisfeats = {}
    vtxfeats = []
    for idx, f in enumerate(feats):
        name, value = mapping[idx]
        if (f == '1'):
            thisfeats[name] = value
        else:
            thisfeats[name] = 'None'
    for ff in featids[1:]:
        vtxfeats.append(thisfeats[ff])
    return vtx(vtxid, *vtxfeats)


# load all of the feature maps, feature files, and self features into an RDD
alledges = sc.emptyRDD()
for personid in peopleids:
        featmap_fname = "/mapr/demo.mapr.com/user/mapr/%d.featnames" % personid
        feats_fname = "%d.feat" % personid
        this_feats_fname = "%d.egofeat" % personid

        # load the feature map
        fmap = []
        with open(featmap_fname) as flines:
            for line in flines:
                fmap.append(fn_process(line))

        # load the features for all the edges, and our own
        f_rdd = sc.textFile(feats_fname).map(lambda x: feat_process(x, -1)). \
             union(sc.textFile(this_feats_fname).map(lambda x: feat_process(x, personid)))
        #f_rdd = sc.textFile(feats_fname).map(lambda x: feat_process(x, -1))

        # add the new data to the group
        alledges = f_rdd.map(lambda x: get_feats(x[0], fmap, x[1])).union(alledges)

# remove duplicates
print "rdd raw count: %d" % alledges.count()

# create a GraphFrame from the result
vdf = sqlContext.createDataFrame(alledges, featids).dropDuplicates(['id'])
print "vertex count: %d" % vdf.count()
print "edge count: %d" % edf.count()
g = GraphFrame(vdf, edf)

# find all connected vertices with the same birthday identifier
print "same birthdays"
res = g.find("(a)-[]->(b)") \
         .filter(featurematch("a.birthday", "b.birthday"))
print "count: %d" % res.count()
res.select("a.id", "a.birthday", "b.id", "b.birthday").show(5)

# find "friends of friends" who are not connected to us, but graduated the same
# year from the same school
print "same class"
res = g.find("(a)-[]->(b); (b)-[]->(c); !(a)-[]->(c)") \
         .filter("%s and %s" % \
                 (featurematch("a.education_school_id", "c.education_school_id"), \
                 featurematch("a.education_year_id", "c.education_year_id")))
res = res.filter("a.id != c.id").select("a.id", "a.education_school_id", "a.education_year_id",
        "c.id", "c.education_school_id", "c.education_year_id") 
print "count: %d" % res.count()
res.show(5)

# finally do a page rank on the graph
print "page rank"
g.pageRank(resetProbability=0.15, tol=0.01).vertices.sort(
    	    'pagerank', ascending=False).show(5)

print "done"
