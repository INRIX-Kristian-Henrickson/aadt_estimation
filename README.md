# aadt_estimation
This repository contains a disconnected set of python scripts and notebooks that were used in the 2019 volume profiles patch. 

Sorry for the state of this, I am gradually adding code and trying to make it more intepretable. I am not including code for the shape development as I feel it is not relavent, of course I can start adding it anyone would like to understand that process. 

The model_aadt_basic.ipynb jupyter notebook is where the AADT modeling takes place. It takes as input the HPMS AADTs, the INRIX Trip counts, and some segment characteristics to predict what is basically a representation of the penetration rate of the trips data by OSM segment. The model features used in the notebook may not be the final feature set that was used in the product, in fact I remember including some representation of the azimuth of the segment. 

create_combine_vp.py was some code that I created (in a zeppelin notebook, so not really deploy-able) to generate the volume profiles from a combination of the estimated AADT, volume shapes, and segment definitiions. It includes some basic rules for when to use the modeled AADTs, when to use the raw HPMS AADTs, and whether to use the simpler or more complex volume shapes (primarily driven by data density). 

The method used to estimate / interpolate AADTs is pretty simple:
1. Get HPMS AADTs and INRIX Trip Paths crossing counts for OSM segments, include segments for which HPMS volumes are not available
2. Compute the count ratio (Trip Count / HPMS AADT), and filter out the extreme values of this ratio
3. Ideally, for each OSM segment we want something like “the average count ratio for nearby, similar segments”. We do this using a KNN model because it was easy to implement in an efficient way. The features include standardized lat/lon, functional class, and some other factors NOT INCLUDING those related to the HPMS data (one of them comes from a probabilistic representation of the arrival process, I can give more details on that but I do not think it added a great deal). 
4. Using the model, predict count ratio for all segment
5. Use a set of heuristics to determine whether the raw HPMS or estimated AADT should be used as the “True” AADT. These heuristics were based mainly on manual investigation, there is really not a lot of science to it. 
