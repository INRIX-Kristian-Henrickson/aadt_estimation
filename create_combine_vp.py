from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType,  ArrayType
import numpy as np
from pyspark.sql.functions import least, greatest

states = 'tx'


sqlContext.sql("set spark.sql.shuffle.partitions={}".format(2000))

sch = 'seg_id STRING, sseg_id STRING, roadname STRING, from_node STRING, to_node STRING, length_m DOUBLE, compseg_id STRING, geometry STRING, frc INT, tz STRING'
dsegs = spark.read.csv('s3://inrixprod-referencedata/data/map=osm/mapversion=20190401/region=na/country=usa/state={}/ref/type=segment/fmt=csv/'.format(states), sep='\t', schema=sch).repartition(50)
dsegs.createOrReplaceTempView('presegs')

sch = 'way_id STRING, maxspeed STRING, roadtype STRING, frc INT, roadname STRING, surface STRING, lanes INT, hov_lanes INT, rt_id STRING, rt_name STRING, rt_role STRING, geometry STRING'
dways = spark.read.csv('s3://inrixprod-referencedata/data/map=osm/mapversion=20190401/region=na/country=usa/state={}/ref/type=way/fmt=csv/'.format(states), sep='\t', schema=sch).repartition(50)
dways.createOrReplaceTempView('preways')

sqs = """
WITH cte AS(
SELECT presegs.seg_id,
        sseg_id,
        from_node,
        to_node,
        length_m,
        preways.frc,
    preways.roadtype,
    ROW_NUMBER() OVER (PARTITION BY seg_id ORDER BY way_id) AS rw
FROM presegs JOIN preways
ON presegs.sseg_id = preways.way_id
WHERE presegs.frc != 6
UNION
SELECT presegs.compseg_id AS seg_id,
        sseg_id,
        from_node,
        to_node,
        length_m,
        preways.frc,
    preways.roadtype,
    ROW_NUMBER() OVER (PARTITION BY seg_id ORDER BY way_id) AS rw
FROM presegs JOIN preways
ON presegs.sseg_id = preways.way_id
WHERE compseg_id != ''
AND presegs.frc != 6
)
SELECT seg_id,
    sseg_id,
    from_node,
    to_node,
    length_m,
    frc,
    roadtype
FROM cte
WHERE rw = 1
"""

df_filter_segments = spark.sql(sqs).repartition(50)
df_filter_segments.createOrReplaceTempView('segs')


df_300_norm = spark.read.parquet('s3://inrixprod-volumes/shapes_development/cluster_results/300_cluster_final_step_07032019/').persist()
df_10_norm = spark.read.parquet('s3://inrixprod-volumes/shapes_development/cluster_results/10_clusters_final_step_07032019/').persist()

df_10_norm.createOrReplaceTempView('c10_norm')
df_300_norm.createOrReplaceTempView('c300_norm')



schema = 'segid STRING, roadname STRING, geometry STRING, hpms_volume DOUBLE, predicted_volume DOUBLE, cross_counts DOUBLE,raw_error DOUBLE, abs_error DOUBLE, perc_error DOUBLE, perc_abs_error DOUBLE, ratio DOUBLE'
        
df_aadt = spark.read.parquet('s3://inrixprod-volumes/shapes_development/patch_aadt_complete/state={}/*'.format(states))
df_aadt.createOrReplaceTempView('preaadts')

sqs = """
SELECT preaadts.*,
    roadtype,
    frc
FROM preaadts JOIN segs
ON preaadts.segid = segs.seg_id
"""

df_aadt_post = spark.sql(sqs).repartition(200)
df_aadt_post.createOrReplaceTempView('aadts')

sqs = """
SELECT    
    c300_norm.seg_id,
    c300_norm.total,
    c300_norm.corrcoef,
    c300_norm.weekday,
    c300_norm.timeofday,
    roadtype,  
    CASE WHEN hpms_volume < 1 THEN predicted_volume * c300_norm.value
        WHEN ( frc = 1 OR frc = 2 ) AND hpms_volume < predicted_volume * 0.5 THEN predicted_volume * c300_norm.value
        WHEN roadtype LIKE '%link' THEN hpms_volume * c300_norm.value
        WHEN perc_abs_error < 0.05 THEN hpms_volume * c300_norm.value
        WHEN ( frc = 1 OR frc = 2 ) AND hpms_volume > predicted_volume THEN hpms_volume * c300_norm.value
        ELSE predicted_volume * c300_norm.value
    END AS volume,
    CASE WHEN hpms_volume < 1 THEN predicted_volume * c10_norm.value
        WHEN ( frc = 1 OR frc = 2 ) AND hpms_volume < predicted_volume * 0.5 THEN predicted_volume * c10_norm.value
        WHEN roadtype LIKE '%link' THEN hpms_volume * c10_norm.value
        WHEN perc_abs_error < 0.05 THEN hpms_volume * c10_norm.value
        WHEN ( frc = 1 OR frc = 2 ) AND hpms_volume > predicted_volume THEN hpms_volume * c10_norm.value
        ELSE predicted_volume * c10_norm.value
    END AS volume10,
    CASE WHEN hpms_volume < 1 THEN predicted_volume
        WHEN ( frc = 1 OR frc = 2 ) AND hpms_volume < predicted_volume * 0.5 THEN predicted_volume
        WHEN roadtype LIKE '%link' THEN hpms_volume
        WHEN perc_abs_error < 0.05 THEN hpms_volume
        WHEN ( frc = 1 OR frc = 2 ) AND hpms_volume > predicted_volume THEN hpms_volume
        ELSE predicted_volume
    END AS aadt,
    '{}' AS state
FROM c300_norm JOIN aadts ON aadts.segid = c300_norm.seg_id
JOIN c10_norm ON aadts.segid = c10_norm.seg_id
AND c10_norm.weekday = c300_norm.weekday
    AND c10_norm.timeofday = c300_norm.timeofday
""".format(states)


dfg = spark.sql(sqs).repartition(500).persist()

dfg.write.parquet('s3://inrixprod-volumes/shapes_development/patch_actual_volumes/state={}/'.format(states))
