#!/usr/bin/env bash

exec prometheus --config.file prometheus.yml --web.listen-address ":9090"