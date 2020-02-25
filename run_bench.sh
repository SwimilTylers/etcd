#!/usr/bin/env bash

exec tools/benchmark/benchmark --endpoints=127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379 --clients=8 --conns=3 --sample put --key-space-size=100000 --total=100000 --rate=200
