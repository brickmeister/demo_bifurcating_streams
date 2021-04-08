# Demo of Delta Lake Bifurcating Streams

This repository contains a demo of ingesting data from a single source and outputting to several delta lake tables utilizing Spark Structured Streaming and Delta lake.

- [Demo of Delta Lake Bifurcating Streams](#demo-of-delta-lake-bifurcating-streams)
- [Architecture](#architecture)
- [Methods](#methods)
  - [Used Python Libraries](#used-python-libraries)
  - [Used Scala Libraries](#used-scala-libraries)
- [Requirements](#requirements)
- [Setup](#setup)
  - [Using Repos Support](#using-repos-support)
  - [Without Repos Support](#without-repos-support)
- [Run the Code](#run-the-code)

# Architecture
![architecture](https://raw.githubusercontent.com/brickmeister/demo_bifurcating_streams/main/img/Bifurcating%20Streams.png)

# Methods

The following code utilizes pyspark and scala spark,.

## Used Python Libraries

* PySpark
* CSV
* Glob
* Zlib

## Used Scala Libraries

* Apache Spark

# Requirements

* [Databricks Account](https://databricks.com/try-databricks)
* [Databricks Repo Support](https://docs.databricks.com/repos.html)
  
# Setup

## Using Repos Support

Ensure you have git support in your Databricks environment. If it is not enabled, please follow the docs in [Databricks Git Integration](https://docs.databricks.com/notebooks/github-version-control.html#get-an-access-token) to get get an access token and link it to your databricks workspace.

One you have repos support, import this [github repo](https://github.com/brickmeister/demo_bifurcating_streams) in the repos ui via the instructions in [linking git repos](https://docs.databricks.com/repos.html#create-a-repo-and-link-it-to-a-remote-git-repository). This should give you access to the notebook and you can just run the notebook in your Databricks workspace.

## Without Repos Support

Import this [notebook](https://github.com/brickmeister/demo_bifurcating_streams/blob/main/notebooks/delta_lake_bifurcated_streams.py) as a URL using the Databricks workspace notebook import UI. Follow [importing a notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook) for more detailed instructions.

# Run the Code

This notebook should run by clicking the `Run All` button in your Databricks Workspace in the Notebook UI.