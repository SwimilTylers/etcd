#!/bin/bash

sudo tc qdisc add dev "${TC_DEV}" root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
sudo tc qdisc add dev "${TC_DEV}" parent 1:2 handle 20: netem "${NETEM_DELAY}"