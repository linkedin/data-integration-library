# Deprecated Job Properties

The following job properties are deprecated, some are no longer being effective, 
and some are replaced by new parameters. 

## ms.watermark.type

ms.watermark.type was designed as an DIL internal property. It is no longer being used. 

ms.watermark.type can cause parsing error if it is used in GaaS flow specs because
its prefix matches another effective parameter ms.watermark. Azkaban projects that still 
have this job property will have no harm. 

