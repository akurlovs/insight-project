# Burst!
### Exploring investment-worthy growth patterns across the U.S.

[Introduction + Rationale](#Introduction-Rationale)

[Data](#Data)

[Tech Stack](#Tech-Stack)

[Engineering Challenge](#Engineering-Challenge)

[MVP](#MVP)

[Stretch Goals](#Stretch-Goals)


## <a name="Introduction-Rationale"></a>Introduction + Rationale
My system will help investors identify metros and other regions of the country that have experienced notable recent growth. The framework will be designed to be scalable and absorb new source of data.

## <a name="Data"></a>Data
I started by collecting monthly and quarterly data from [The Bureau of Labor Statistics](https://www.bls.gov/data/)

## <a name="Tech-Stack"></a>Tech Stack
Data imported into [Amazon S3](https://aws.amazon.com/s3/) was batched-processed into a [PostgreSQL](https://www.postgresql.org/) using [Apache Spark](https://spark.apache.org/), and visualized using [Dash by plotly](https://plot.ly/dash/)

## <a name="#Engineering-Challenge"></a>Engineering Challenge
The source data is hard or impossible to query for specific information. Also, some of the data (for example, industry descriptions) can be redundant, inconsistent, and generally don't come in a neat fashion.

## <a name="MVP"></a>MVP
Minimally, I will build an infrastructure to easily locate industry growth leaders across the country

## <a name="Stretch-Goals"></a>Stretch Goals
In an ideal world, this would grow into a general platform for easier access to government research.

