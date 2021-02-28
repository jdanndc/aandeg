#!/bin/bash
python aandeg/util/depend_dot.py | dot -Tpng -o depend.png ; open depend.png
