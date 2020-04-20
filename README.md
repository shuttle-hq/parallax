![parallax ci/cd](https://github.com/openquery-io/core-private/workflows/parallax%20ci/cd/badge.svg) ![parallax tests](https://github.com/openquery-io/core-private/workflows/parallax%20tests/badge.svg) [![version: 0.1](https://img.shields.io/badge/version-0.1-orange.svg)](./) [![license: BSL1.1](https://img.shields.io/badge/license-BSL1.1-green.svg)](./LICENSE)

# Parallax

**Parallax** is a distributed query engine for private data. It is:

- **Privacy as code**: It lets you write privacy policies for your data subjects in a simple YAML manifest and enforces them rigorously.
- **Distributed**: It scales as much as the database where your data is.
- **Declarative**: You state what you need anonymized and it figures out how to do it. With no unpredictable side-effect and with complete reproducibility.

Check out an [example manifest](example/manifest/london_bicycles.yaml) to see what we mean.

# Getting Started

The fastest way to get hacking with Parallax is to run it straight from our distributed image
```
docker run -it --rm eu.gcr.io/openquery/parallax
```
The image runs a single `parallax-worker` node in the background (alongside a small redis box, which we use as a basic transaction database)  and lets you issue commands against it using the `parallax` command-line interface. The first thing you will want to do is initialize the deployment with
```
parallax init
```
which will configure an initial cluster state for us. That's it! We're up and running with the basics.

Should you wish to quickly onboard some data so we can start doing some queries, the easiest is to use the example manifests under [example/manifest/](example/manifest/).

In this guide, we'll link up with a public dataset on BigQuery. For this part you will need a [service account key](https://cloud.google.com/iam/docs/service-accounts) for GCP handy so that Parallax can authenticate to run queries on the data on your behalf. That service account will need [write access](https://cloud.google.com/bigquery/docs/access-control-primitive-roles) to a BigQuery dataset so Parallax has somewhere to store staging artifacts. The data in the examples average ~5gb in size so you should comfortably stay within the [BigQuery free-tier](https://cloud.google.com/bigquery/pricing) of 1GB (at the time of writing this). 

Let's use the [london_bicycles](https://console.cloud.google.com/marketplace/details/greater-london-authority/london-bicycles?filter=solution-type:dataset&id=95374cac-2834-4fa2-a71f-fc033ccb5ce4) dataset for which we have a made a preset collection of example policies in [example/manifest/london_bicycles.yaml](example/manifest/london_bicycles.yaml). We first need to get the current state of the cluster stored in a file before we can add the dataset specific entries. For this we run
```bash
parallax --disable-tls gov fetch --output=state.yaml
```
which will create the file `state.yaml` which contains a manifest of all the resources under Parallax's control (at this point just the `root` user and an administrators' group called `wheel`). Note that we have to use `--disable-tls` as the worker node on the image has TLS disabled by default.

Copy your service account key into the running container (see [docker cp](https://docs.docker.com/engine/reference/commandline/cp/)) and set the following environment variables
```bash
export SERVICE_ACCOUNT_KEY={where my key is}
export STAGING_PROJECT_ID={a GCP project_id}
export STAGING_DATASET_ID={a GCP dataset_id}
```
We can then append the example manifest to our state with
```bash
curl https://raw.githubusercontent.com/openquery-io/core/master/parallax/example/manifest/london_bicycles.yaml |\
    envsubst >> state.yaml
```
Finally we have to update the state of Parallax so that it uses the new manifest. This is done with
```bash
parallax --disable-tls gov apply
```
and we're done! we can start querying the protected dataset. Queries are managed via the `jobs` subcommand. For example
```bash
cat<<EOF | parallax jobs insert
SELECT start_station_id, COUNT(rental_id) 
FROM safe_dataset.cycle_hire
GROUP BY start_station_id
EOF
```
We can find out the state of submitted jobs with `parallax jobs list` and `parallax jobs get`. When it is done we can fetch the result as a csv with
```bash
parallax jobs fetch {JOB_ID} --format=csv
```
The `init` command we ran at the very beginning also set the corresponding credentials for the CLI. These can be managed with the `list` subcommand
```bash
parallax --disable-tls auth list
```
All credentials get stored under `$PARALLAX_HOME` (by default `~/.config/parallax`).

# Building Parallax

You will first need to clone this repository locally, including all the submodules with,
```bash
git clone --recursive git@github.com:openquery-io/core.git openquery-core
```

## Building in Docker
The easiest is to build the whole thing in Docker. Simply navigate to the root of the `core` repository and run 
```bash
docker build -f parallax/docker/Dockerfile.build .
```

## Building locally
Navigate to `parallax/` and run `cargo build`. Building Parallax requires Rust nightly.

# Running tests

Most tests can be run locally as usual with `cargo test`. Some will require additional credentials to query which by default have to supplied by filling in the `secret/` folder at the root of the repository (currently used by the BigQuery backend looking for a GCP service account key to run under). The easiest way to find out what is missing is trying to run them and filling them in as required. We're actually working on a solution to make these publically runnable using our credentials so hang tight!

Apart from that you will need a quick and dirty Redis box for integration tests. This can be done simply by running off the official Redis docker image. For example:
```bash
docker run -it --rm -p 6379:6379 redis
```

## Contributing

First of all, we sincerely appreciate any contributions large or small - so thank you.

See the [contributing](CONTRIBUTING.md) page for more details.

## License

This project is licensed under the [BSL license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Parallax by you, shall be licensed as [BSL license](LICENSE), without any additional
terms or conditions.

