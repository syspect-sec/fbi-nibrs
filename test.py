#!/usr/bin/env python
# -*- coding: utf-8 -*-
input = "/Users/development/Software/NIBRS/TMP/downloads/AL-1991/nibrs_arrestee_weapon.csv"
output = "/Users/development/Software/NIBRS/TMP/downloads/AL-1991/nibrs_arrestee_weapon_2.csv"
file_list = []
with open(input, "r") as infile:
    for line in infile:
        line = line.strip().split(",")
        line = "|".join(line)
        file_list.append(line + "\n")

with open(output, "w") as outfile:
    for line in file_list:
        outfile.write(line)
