#! /usr/bin/env sh

postgres=$(docker run \
    -e POSTGRES_DB=lsst \
    -e POSTGRES_USER=ampel \
    -e POSTGRES_PASSWORD=seekrit \
    -d \
    -P \
    --rm \
    postgres:18.0)

host_port() {
    docker inspect $1 | jq -r '.[0].NetworkSettings.Ports["'$2'"][0].HostPort'
}

POSTGRES_URI=postgresql://ampel:seekrit@localhost:$(host_port $postgres "5432/tcp")/lsst


localstack=$(docker run \
    -e SERVICES=s3 \
    -e DEBUG=s3 \
    -d \
    -P \
    --rm \
    localstack/localstack:0.12.19.1)

LOCALSTACK_URI=http://localhost:$(host_port $localstack "4566/tcp")


cleanup() {
    echo stopping services...
    docker stop $postgres $localstack
}
trap cleanup SIGINT

echo postgres: $postgres
echo localstack: $localstack
echo POSTGRES_URI=\"$POSTGRES_URI\"
echo LOCALSTACK_URI=\"$LOCALSTACK_URI\"
echo ARCHIVE_URI=\"$POSTGRES_URI\"

docker wait $postgres $localstack
