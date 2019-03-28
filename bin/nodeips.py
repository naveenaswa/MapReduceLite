#!/usr/bin/env python
import sys
import json

result = json.load(sys.stdin)
for node in result['Instances']:
	ip =  node['PrivateIpAddress'];
	print ip
	
