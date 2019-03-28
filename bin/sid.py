#!/usr/bin/env python
import sys
import json
try:
	result = json.load(sys.stdin)
except:
	print "error"
	exit()
	
stepid =  result['GroupId']
print stepid
