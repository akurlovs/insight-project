# Burst!
### Exploring investment-worthy growth patterns across the U.S.

[Introduction + Rationale](#Introduction-Rationale)

[Data](#Data)

[Tech Stack](#Tech-Stack)

[Engineering Challenge](#Engineering-Challenge)

[Stretch Goals](#Stretch-Goals)

[Front End Screencast](#Front-End)


## <a name="Introduction-Rationale"></a>Introduction + Rationale
This project helps identify metros and other regions of the country that have experienced growth in particular industries, and thus helps connect potential investors (and also employees!) to the places where the industry is located.

## <a name="Data"></a>Data
Monthly and quarterly data from [The Bureau of Labor Statistics](https://www.bls.gov/data/)

## <a name="Tech-Stack"></a>Tech Stack
Data imported into [Amazon S3](https://aws.amazon.com/s3/) was batched-processed into a [PostgreSQL](https://www.postgresql.org/) using [Apache Spark](https://spark.apache.org/), and visualized using [Dash by plotly](https://plot.ly/dash/)

## <a name="#Engineering-Challenge"></a>Engineering Challenge
The source data is hard to query for specific information. Also, some of the data (for example, industry descriptions) can be redundant, inconsistent, and generally don't come in a neat fashion.

## <a name="Stretch-Goals"></a>Stretch Goals
In an ideal world, this would grow into a general platform for easier access to government research.

## <a name="Front-End"></a>Front End

<video src="front_end/screencast.mp4" width="320" height="200" controls preload></video>
