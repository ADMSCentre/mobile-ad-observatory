'''
	a distributed cache has a read and a write function just like any other cache
	HOWEVER
	it also has the concession of loading up partially, or loading up entirely
		in the partial case, it loads up the last few days
		in the non-partial case, it loads up everything
	consider initial creation:
		we section up results by days
	reading then involves scanning over the entire bucket of days OR scanning only over the timeline window
	writing involves writing to only the part that is affected
'''

import sys
import os
import time
if (__name__ == "__main__"):
	import ipdb
import re
import boto3
import math
import uuid
import json
import random
import traceback
import botocore
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError

##############################################################################################################################
##############################################################################################################################
### AWS
##############################################################################################################################
##############################################################################################################################

# Load up the necessary AWS infrastructure
# Note: On remote infrastructures, we don't authenticate as the Lambda handler will have the necessary
# permissions built into it

AWS_CONFIG = {
		"aws" : {
			"AWS_PROFILE" : "dmrc",
			"AWS_REGION" : "ap-southeast-2"
		}
	}

def aws_load(running_locally=False):
	credentials_applied = dict()
	if (running_locally):
		# Running locally
		credentials = boto3.Session(profile_name=AWS_CONFIG["aws"]["AWS_PROFILE"]).get_credentials()
		credentials_applied = {
				"region_name" : AWS_CONFIG["aws"]["AWS_REGION"],
				"aws_access_key_id" : credentials.access_key,
				"aws_secret_access_key" : credentials.secret_key
			}
	AWS_RESOURCE = {k : boto3.resource(k, **credentials_applied) for k in ["s3"]}
	AWS_CLIENT = {k : boto3.client(k, **credentials_applied) for k in ["s3"]}
	return AWS_CLIENT, AWS_RESOURCE

AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

A_DAY = 24 * 60 * 60

def get_list_objects_v2(Bucket=None, Prefix=None):
	result = list()
	paginator = AWS_CLIENT["s3"].get_paginator('list_objects_v2')
	pages = paginator.paginate(Bucket=Bucket, Prefix=Prefix)
	for page in pages:
		if ("Contents" in page):
			result.extend(page['Contents'])
	return {"Contents" : result}


'''
	This function determines the contents of a subbucket within an S3 bucket
'''
def subbucket_contents(kwargs, search_criteria="CommonPrefixes"):
	results = list()
	if (search_criteria == "CommonPrefixes"):
		results = [x for x in [prefix if (prefix is None) else prefix.get("Prefix") 
			for prefix in AWS_CLIENT["s3"].get_paginator("list_objects_v2").paginate(
				**{**{"Delimiter" : "/"}, **kwargs}).search("CommonPrefixes")] if (x is not None)]
	else:
		try:
			for batch_obj in [x for x in AWS_CLIENT["s3"].get_paginator("list_objects_v2").paginate(**{**{"Delimiter" : "/"}, **kwargs})]:
				for key_obj in batch_obj["Contents"]:
					results.append(key_obj["Key"])
		except:
			pass
			# Bucket is probably empty
	return results
'''
	This function traverses a path
'''
def traverse_path(obj, path):
	cur = obj
	for key in path:
		try:
			if isinstance(cur, dict):
				cur = cur[key]
			elif isinstance(cur, (list, tuple)):
				cur = cur[key]
			else:
				return None
		except (KeyError, IndexError, TypeError):
			return None
	return cur

def distributed_cache_exists_s3_wrapper(this_bucket, this_path):
	try:
		AWS_CLIENT["s3"].head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False


'''
	This function accepts a cache, and then distributes it accordingly

	Note: It is assumed that distributed caches are dictionaries, with a longitudinal unit

	Note: This routine should be undertaken locally

	Eg.

		Nominal case:

		inject_distributed_cache({
				"from" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"key" : "ccl_cache.json"
				},
				"to" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				},
				"longitudinal_unit" : A_DAY,
				"longitudinal_key" : ["timestamp"],
				"key" : ["uuid"]
			})

		Categorical case:

		inject_distributed_cache({
				"from" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"key" : "ccl_data_donation_cache_processed.json"
				},
				"to" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				},
				"categorical" : True,
				"longitudinal_key" : ["observer_uuid"]
			})
'''
def inject_distributed_cache(params):
	appendage = str() if (not "categorical" in params) else "_categorical"
	source_cache = None
	if (not os.path.exists(f"source_cache{appendage}.json")):
		source_cache = json.loads(AWS_RESOURCE["s3"].Object(params["from"]["bucket"], params["from"]["key"]).get()['Body'].read())
		with open("source_cache.json", "w") as f: f.write(json.dumps(source_cache, indent=3))
	source_cache = json.loads(open(f"source_cache{appendage}.json").read())

	# Retrieve the longitudinal units for all entries
	l_values = list()
	for k in source_cache:
		this_entry = source_cache[k]
		this_l_value = traverse_path(this_entry, params["longitudinal_key"])
		l_values.append(this_l_value)

	if ("categorical" in params):
		# Categorical case

		l_values = list(set(l_values)) # Make distinct
		# For the distributants
		distributants = {k:dict() for k in l_values}
		# Populate the distributants
		for k in source_cache:
			x = source_cache[k]
			distributants[traverse_path(x, params["longitudinal_key"])][k] = x
	else:
		# Nominal case

		# Divide the earliest entry by the unit to derive a starting index
		starting_distributant_unit = int(math.floor(min(l_values) / params["longitudinal_unit"]) * params["longitudinal_unit"])

		# Then convert the source cache into a list of entries, sorted by the longitudinal unit
		source_cache_listified_sorted = sorted([v for v in source_cache.values()], key=lambda x: traverse_path(x, params["longitudinal_key"]))

		# Then for each entry, designate it to a longitudinal bucket
		distributants = dict()
		current_distributant_unit = str(starting_distributant_unit)
		threshold_unit = starting_distributant_unit + params["longitudinal_unit"]
		for x in source_cache_listified_sorted:
			# Get the longitudinal unit for the current entry
			current_longitudinal_unit = traverse_path(x, params["longitudinal_key"])
			# Adjust the current distributant unit if the current entry's longitudinal unit exceeds it
			while (current_longitudinal_unit >= threshold_unit):
				current_distributant_unit = str(threshold_unit)
				threshold_unit += params["longitudinal_unit"] 
			# Add the current distributant to the distributants (if it isn't already indexed)
			if (not current_distributant_unit in distributants):
				distributants[current_distributant_unit] = dict()
			# Apply the value
			distributants[current_distributant_unit][traverse_path(x, params["key"])] = x

	for distributant_key in distributants:
		AWS_RESOURCE["s3"].Object(params["to"]["bucket"], f'{params["to"]["path"]}/{distributant_key}.json').put(Body=json.dumps(distributants[distributant_key], indent=3))


'''
	This function attempts to read a direct key from S3 - failing when it is isn't available
'''
def distributed_cache_read_s3_wrapper(bucket, key, n_attempt=int(), n_seconds_to_sleep=2):
	try:
		return json.loads(AWS_RESOURCE['s3'].Object(bucket, key).get()['Body'].read())
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
			if (n_attempt < 3):
				time.sleep(n_seconds_to_sleep)
				return distributed_cache_read_s3_wrapper(bucket, key, n_attempt=n_attempt+1, n_seconds_to_sleep=n_seconds_to_sleep)
			else:
				print(f"❌ Attempt to bypass throttling has failed")
				raise Exception()
		elif error_code == 'NoSuchKey':
			print(f"Could not find cache '{key}' in '{bucket}'")
			raise Exception()
		else:
			print(f"❌ Other S3 error: {e}")
			raise Exception()
	except:
		raise Exception()

def distributed_cache_write_s3_wrapper(bucket, key, value, n_attempt=int(), n_seconds_to_sleep=2):
	try:
		AWS_RESOURCE['s3'].Object(bucket, key).put(Body=json.dumps(value, indent=3))
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
			if (n_attempt < 3):
				time.sleep(n_seconds_to_sleep)
				distributed_cache_write_s3_wrapper(bucket, key, value, n_attempt=n_attempt+1, n_seconds_to_sleep=n_seconds_to_sleep)
			else:
				print(f"❌ Attempt to bypass throttling has failed")
				raise Exception()
		else:
			print(f"❌ Other S3 error: {e}")
		raise Exception()
	except:
		raise Exception()
		print(traceback.format_exc())
		pass

def distributed_cache_exists_s3_wrapper(bucket, key):
	try:
		AWS_CLIENT["s3"].head_object(Bucket=bucket, Key=key)
		return True
	except:
		return False
	return False

'''
	This function reads a distributed cache

	Eg.

		Nominal case:

		distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				},
				"read_range" : [1742947200, 1744675200]
			})

		Categorical case:

		distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				},
				"read_keys" : ["ff38b2c5-c7b8-4308-ba35-720225b38679", "744a0439-b6aa-49ea-811d-37efed4e7cc8"]
			})
'''
def distributed_cache_read(params):
	distributants = get_list_objects_v2(Bucket=params["cache"]["bucket"], Prefix=params["cache"]["path"])
	distributants_keys = list()
	if ("Contents" in distributants): distributants_keys = [x["Key"] for x in distributants["Contents"]]
	output = dict()
	applied_distributants_keys = list(distributants_keys)
	# Apply the read range if necessary
	if ("read_range" in params):
		tentative_keys = list()
		for x in applied_distributants_keys:
			interpreted_longitudinal_value = int(x.split("/")[-1].replace(".json", str()))
			if ((interpreted_longitudinal_value >= params["read_range"][0]) and (interpreted_longitudinal_value <= params["read_range"][1])):
				tentative_keys.append(x)
		applied_distributants_keys = list(tentative_keys)
	elif ("read_keys" in params):
		applied_distributants_keys = [f'{params["cache"]["path"]}/{x}.json' for x in params["read_keys"]]
	# Load the results into a single output file
	for k in applied_distributants_keys:
		# The categorical case introduces the possibility of attempting to read direct distributants explicitly by name, which may not exist - to handle this,
		# we introduce a s3 object check...
		if (distributed_cache_exists_s3_wrapper(params["cache"]["bucket"], k)):
			output |= distributed_cache_read_s3_wrapper(params["cache"]["bucket"], k)
	#with open("output-all.json", "w") as f: f.write(json.dumps(output, indent=3))
	return output

'''
	This function writes to a distributed cache

	Eg. Nominal case

	distributed_cache_write({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_cache_distributed"
			},
			"longitudinal_unit" : A_DAY,
			"longitudinal_key" : ["timestamp"],
			"input" : {
				"76d428e1-2864-4e48-ae13-aa42a8ae1fd4": {
			      "uuid": "76d428e1-2864-4e48-ae13-aa42a8ae1fd4",
			      "term": "conm",
			      "group_i": 0,
			      "platform": "INSTAGRAM",
			      "ad_type": "REEL_BASED",
			      "observer_uuid": "cf991f1e-e7db-4562-8999-0f4117fb962a",
			      "group_uuid": "870c8bca-521e-4e52-94f1-3781c6422ada",
			      "timestamp": 1744777947.0,
			      "outcome": {
			         "at": 1761794473,
			         "version": 1000,
			         "status": "SCRAPED",
			         "mass_downloaded": False,
			         "foo" : "bar"
			      }
			   },
			   "d97d927e-2b5c-4348-9aa2-5bf0b52187b2": {
			      "uuid": "d97d927e-2b5c-4348-9aa2-5bf0b52187b2",
			      "term": "No in-person visits:",
			      "group_i": 1,
			      "platform": "INSTAGRAM",
			      "ad_type": "REEL_BASED",
			      "observer_uuid": "cf991f1e-e7db-4562-8999-0f4117fb962a",
			      "group_uuid": "870c8bca-521e-4e52-94f1-3781c6422ada",
			      "timestamp": 1744778053.8782609,
			      "outcome": {
			         "at": 1761794509,
			         "version": 1000,
			         "status": "SCRAPED",
			         "mass_downloaded": False,
			         "foo" : "bar"
			      }
			   }
			}
		})

	Eg. Categorical case
	
	distributed_cache_write({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_data_donation_cache_distributed"
			},
			"categorical" : True,
			"longitudinal_key" : ["observer_uuid"],
			"input" : {
			   "0020c67f-fc44-455b-a62d-1d4039c8c492/b53a23f3-2ce7-4814-896c-0b21b5284c4a.json": {
			      "group_uuid": "8511a137-d18b-4ddd-9aaf-66d7bc3f6f00",
			      "group_term_uuids": [
			         "7d06be08-9e3f-47cb-a6c9-bbe456d766f9"
			      ],
			      "observer_uuid": "0020c67f-fc44-455b-a62d-1d4039c8c492"
			   },
			   "0020c67f-fc44-455b-a62d-1d4039c8c492/c5e47700-0538-426c-8ff4-ad0b069ede74.json": {
			      "group_uuid": "8511a137-d18b-4ddd-9aaf-66d7bc3f6f00",
			      "group_term_uuids": [
			         "9558d52d-8a95-46a0-a952-8c0529d65164"
			      ],
			      "observer_uuid": "0020c67f-fc44-455b-a62d-1d4039c8c492",
			      "foo" : "bar"
			   },
			   "undefined/29380186-c559-4e77-8c25-07f1c09e3c5d.json": {
			      "group_uuid": "4147fa81-497c-43aa-82b6-60e4a840185f",
			      "group_term_uuids": [
			         "819f9855-6efa-4d56-b857-9210b539cff1"
			      ],
			      "observer_uuid": "undefined",
			      "foo" : "bar"
			   }
		   }
		})
'''
def distributed_cache_write(params):
	# Get all longitudinal units
	distributants_to_get = list()
	inputs_to_distributants = dict()
	if ("categorical" in params):
		for this_key in params["input"]:
			this_distributant = traverse_path(params["input"][this_key], params["longitudinal_key"])
			this_distributant = f'{params["cache"]["path"]}/{this_distributant}.json'
			distributants_to_get.append(this_distributant)
			inputs_to_distributants[this_key] = this_distributant
		distributants_to_get = list(set(distributants_to_get))
	else:
		for this_key in params["input"]:
			this_longitudinal_unit = traverse_path(params["input"][this_key], params["longitudinal_key"])
			# Determine which distributant it should go to
			this_distributant = int(math.floor(this_longitudinal_unit / params["longitudinal_unit"]) * params["longitudinal_unit"])
			this_distributant = f'{params["cache"]["path"]}/{this_distributant}.json'
			distributants_to_get.append(this_distributant)
			inputs_to_distributants[this_key] = this_distributant
		distributants_to_get = list(set(distributants_to_get))

	# Find and load all affected distributants
	distributants = dict()
	for this_distributant_key in distributants_to_get:
		# If it exists
		if (distributed_cache_exists_s3_wrapper(params["cache"]["bucket"], this_distributant_key)):
			distributants[this_distributant_key] = distributed_cache_read_s3_wrapper(params["cache"]["bucket"], this_distributant_key)
		else:
			distributants[this_distributant_key] = dict()

	# Modify each distributant
	for this_key in inputs_to_distributants:
		# Complete overwrite
		if ("delete" in params):
			del distributants[inputs_to_distributants[this_key]][this_key]
		else:
			distributants[inputs_to_distributants[this_key]][this_key] = params["input"][this_key]

	# And finally commit the results...
	for this_distributant_key in distributants_to_get:
		# Note we sort before committing
		distributants[this_distributant_key] = {k: v for k, v in 
			sorted(distributants[this_distributant_key].items(), key=lambda x: traverse_path(x[1], params["longitudinal_key"]), reverse=False)}
		distributed_cache_write_s3_wrapper(params["cache"]["bucket"], this_distributant_key, distributants[this_distributant_key])


def distributed_cache_read_range_auto(this_timestamp, this_unit=A_DAY):
	lower_bound = int(math.floor(this_timestamp / this_unit) * this_unit)
	upper_bound = (lower_bound + this_unit)
	return [lower_bound, upper_bound]


'''
	FOR IMPLEMENTING A DELETION:
		distributed_cache_write({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				},
				"longitudinal_unit" : A_DAY,
				"longitudinal_key" : ["timestamp"],
				"delete" : True,
				"input" : {
					"dummy": {
				      "uuid": "dummy",
				      "term": "a dummy term that shouldnt mean anything",
				      "group_i": 0,
				      "platform": "INSTAGRAM",
				      "ad_type": "REEL_BASED",
				      "observer_uuid": "cf991f1e-e7db-4562-8999-0f4117fb962a",
				      "group_uuid": "870c8bca-521e-4e52-94f1-3781c6422ada",
				      "timestamp": 1744777947.0,
				      "outcome": {
				         "at": 1761794473,
				         "version": 1000,
				         "status": "SCRAPED",
				         "mass_downloaded": False
				      }
				   }
				}
			})

'''


if (__name__ == "__main__"): pass




