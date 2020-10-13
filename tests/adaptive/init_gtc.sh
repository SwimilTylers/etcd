#!/bin/bash

sudo tc qdisc add dev lo root netem delay 10ms 5ms distribution normal