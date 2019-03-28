#!/usr/bin/env python
import sys
import json

result = json.load(sys.stdin)
#print result
State =  result['Cluster']['Status']['State']
print State
	
