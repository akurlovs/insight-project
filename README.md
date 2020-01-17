# insight-project
My Data Enginering Project for Insight

# Introduction + Rationale
Metro areas that grow "organically" can be negatively impacted by rapid growth -- take, for instance, The Bay Area that suffers from astronomically high rents and homelessness, or the chronic and seemingly unsolvable traffic problems in Los Angeles. These issues can be offset by developing other metros and even rural areas in a smart, data-driven way. The U.S. government does an excellent job collecting the kinds of data that can help make those decisions. However, much of that data is fragmented and is not accessible in a comprehensive, user-friendly way. 

My system will help investors identify metros and other regions of the country that have experienced notable recent growth and/or are local industry leaders. The system will be designed to be scalable and absorb new data as it comes in, as investors may also be interested in other variables such as the local government's investement in infrastructure or the fraction of the population that is young and educated.

My hope is that my Insight project can serve as an inspiration for increasing transparancy and exchange of ideas that can lead to creating sustainable prosperity.

# Data 
I will collect data from [The Bureau of Labor Statistics](https://www.bls.gov/data/) and [The Census Bureau](https://www.census.gov/data.html), among other sources.

# Tech Stack
Data will be imported into [Amazon S3](https://aws.amazon.com/s3/) and batched-processed into a [PostreSQL](https://www.postgresql.org/) using [Apache Spark](https://spark.apache.org/). Plotting will be accomplished using software that will allow me to overlay a map of the U.S. (have not figured out which one yet; suggestions appreciated).

# Engineering Challenge
I anticipate several challenges in the course of this project. First, the data come from diverse schema, and I will need to use a bit of ingenuity to figure out how to combine it effeciently and intelligently. Second, some of the data (for example, industry descriptions) can be redundant and generally don't come in a neat fashion. Third, the database needs to be updated regularly as the Bureau of Labor, for instance, releases some data monthly and other data quarterly.

# MVP
Minimally, I will build an infrastructure to easily locate industry and population microbursts across the country.

# Stretch Goals
In an ideal world, this would grow into a general platform for easiero access to government research.

