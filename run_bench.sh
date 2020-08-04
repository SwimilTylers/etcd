#!/usr/bin/env bash

#exec tools/benchmark/benchmark --endpoints=127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 --clients=27 --conns=3 --sample put --key-size=8 --sequential-keys --total=100000 --val-size=256

size=3

bash ./run_bench_"${size}".sh
bash ./run_bench_verify_"${size}".sh