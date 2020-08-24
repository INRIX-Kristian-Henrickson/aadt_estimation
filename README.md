# aadt_estimation

The method used to estimate / interpolate AADTs is pretty simple:
1.	Get HPMS AADTs and INRIX Trip Paths crossing counts for OSM segments, include segments for which HPMS volumes are not available
2.	Compute the count ratio (Trip Count / HPMS AADT), and filter out the extreme values of this ratio
3.	Ideally, for each OSM segment we want something like “the average count ratio for nearby, similar segments”. We do this using a KNN model because it was easy to implement in an efficient way. The features include standardized lat/lon, functional class, and some other factors NOT INCLUDING those related to the HPMS data (one of them comes from a probabilistic representation of the arrival process, I can give more details on that but I do not think it added a great deal). 
4.	Using the model, predict count ratio for all segment
5.	Use a set of heuristics to determine whether the raw HPMS or estimated AADT should be used as the “True” AADT. These heuristics were based mainly on manual investigation, there is really not a lot of science to it. 
