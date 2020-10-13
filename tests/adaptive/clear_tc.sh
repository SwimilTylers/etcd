#!/bin/bash

sudo tc qdisc del dev "${TC_DEV}" parent 1:2 handle 20: netem
sudo tc qdisc del dev "${TC_DEV}" root handle 1: prio