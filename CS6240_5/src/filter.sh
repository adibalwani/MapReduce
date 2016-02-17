#!/bin/bash
# Author: Rachit Puri

awk '{print $1 " " $2 " " $4 " " $5}' test | sort -t, -nk1 > solution