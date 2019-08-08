#!/usr/bin/env bash
set -euo pipefail

### Functions

time_elapsed(){
	printf "\033[36m(%02d:%02d)\033[0m" $[SECONDS/60] $[SECONDS%60]
}

fail_if_port_busy() {
    local PORT=$1
    if nc -z localhost $PORT ; then
        echo "Port $PORT is busy. Exiting"
        exit 1
    fi
}

get_hpc_files() {
	python3 -c 'import sqlite3
with sqlite3.connect("'$HASURA_TEST_INFO_DB'") as conn:
  for x in conn.execute("select * from hpc_files").fetchall():
    print(x[0])
'
}
  
combine_all_hpc_reports() {
	combined_file="${HASURA_TEST_OUTPUT_FOLDER}/graphql-engine.tix"
	combined_file_intermediate="${HASURA_TEST_OUTPUT_FOLDER}/hpc/graphql-engine-combined-intermediate.tix"
	rm -f "$combined_file"
	for tix_file in $(get_hpc_files)
	do
		if ! [ -f "$tix_file" ] ; then
			continue
		fi
		if [ -f "$combined_file" ]  ; then
			(set -x && stack --allow-different-user exec -- hpc combine "$combined_file" "$tix_file" --union --output="$combined_file_intermediate" && set +x && mv "$combined_file_intermediate" "$combined_file" && rm "$tix_file" ) || true
		else
			mv "$tix_file" "$combined_file" || true
		fi
	done
}

if [ -z "${HASURA_GRAPHQL_DATABASE_URL:-}" ] ; then
	echo "Environment variable HASURA_GRAPHQL_DATABASE_URL is not set"
	exit 1
fi

if [ -z "${HASURA_GRAPHQL_DATABASE_URL_2:-}" ] ; then
	echo "Env var HASURA_GRAPHQL_DATABASE_URL_2 is not set"
	exit 1
fi

if ! stack --allow-different-user exec which hpc ; then
	echo "hpc not found; Install it with 'stack install hpc'"
	exit 1
fi

CIRCLECI_FOLDER="${BASH_SOURCE[0]%/*}"
cd $CIRCLECI_FOLDER
CIRCLECI_FOLDER="$PWD"

if ! $CIRCLECI_FOLDER/test-server-flags.sh ; then
	echo "Testing GraphQL server flags failed"
	exit 1
fi

if ! $CIRCLECI_FOLDER/test-deprecated-server-flags.sh ; then
	echo "Testing GraphQL deprecated server flags failed"
	exit 1
fi

PYTEST_ROOT="$CIRCLECI_FOLDER/../server/tests-py"

export HASURA_TEST_OUTPUT_FOLDER=${HASURA_TEST_OUTPUT_FOLDER:-"$CIRCLECI_FOLDER/test-server-output"}
mkdir -p "$HASURA_TEST_OUTPUT_FOLDER"

cd $PYTEST_ROOT

pip3 install tox

if ! stack exec -- which graphql-engine > /dev/null && [ -z "${HASURA_TEST_GRAPHQL_ENGINE:-}" ] ; then
	echo "Do 'stack build' before tests, or export the location of executable in the HASURA_TEST_GRAPHQL_ENGINE envirnoment variable"
	exit 1
fi

export HASURA_TEST_GRAPHQL_ENGINE=${HASURA_TEST_GRAPHQL_ENGINE:-"$(stack exec -- which graphql-engine)"}
if ! [ -x "$HASURA_TEST_GRAPHQL_ENGINE" ] ; then
	echo "$HASURA_TEST_GRAPHQL_ENGINE is not present or is not an executable"
	exit 1
fi

for port in 8080 8081 9876 5592
do
	fail_if_port_busy $port
done

echo -e "\nINFO: GraphQL Executable : $HASURA_TEST_GRAPHQL_ENGINE"
echo -e   "INFO: Logs Folder        : $HASURA_TEST_OUTPUT_FOLDER\n"

export HASURA_TEST_INFO_DB="$HASURA_TEST_OUTPUT_FOLDER/test_info.db"
rm -f $HASURA_TEST_INFO_DB

export HASURA_TEST_HGE_RTS_OPTS='-N2'

p='pgUrl-hgeExec'

#Use two databases for parallel tests. Reduces the time by half. 
#Most of the tests on event triggers involve a lot of waiting. 
#The event trigger tests can be run on one and the rest on the another one.
export HASURA_TEST_PG_URLS="$HASURA_GRAPHQL_DATABASE_URL,$HASURA_GRAPHQL_DATABASE_URL_2"
for env in $p-{no,adminSecret,jwt,{get,post}Webhook}Auth-default
do
	echo -e -n "$(time_elapsed) "
	set -x
	python3 -m tox -v -e $env
	set +x
done

#These are much smaller special case tests. Parallelism is not required
export HASURA_TEST_PG_URLS="$HASURA_GRAPHQL_DATABASE_URL"
for env in $p-websocket{{,No}ReadCookieCorsDisabled,ReadCookieCorsEnabled} \
           $p-corsDomains $p-{graphql,metadata}ApiDisabled{Env,Arg} \
 	   $p-allowListEnabled{Env,Arg} $p-horizontalScaling $p-queryLogs \
           $p-insecure{Post,Get}Webhook $p-jwtStringified \
           $p-jwtWith{Issuer,Audience{,List}} ; do
	echo -e -n "$(time_elapsed) "
	set -x
	python3 -m tox -v -e $env
	set +x
done

combine_all_hpc_reports || true

echo -e "\n$(time_elapsed): <########## DONE ########>\n"
