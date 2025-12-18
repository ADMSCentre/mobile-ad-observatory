'''
	Accepts raw tentative ads
'''

import sys
import os
import time
if (__name__ == "__main__"):
	import ipdb
import json
import boto3
import botocore
import base64
import traceback


s3_client = boto3.client('s3', region_name='ap-southeast-2')

s3 = boto3.resource('s3')
S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"

template_quick_access_cache = {
			"observations" : list(),
			"ads" : list(),
			"ads_passed_ocr" : list(),
			"ads_passed_ad_scrape" : list(),
			"ads_passed_mass_download" : list()
		}

template_entrypoint_data_donation = {"evaluated":False}


def get_list_objects_v2(Bucket=None, Prefix=None):
	result = list()
	paginator = s3_client.get_paginator('list_objects_v2')
	pages = paginator.paginate(Bucket=Bucket, Prefix=Prefix)
	for page in pages:
		if ("Contents" in page):
			result.extend(page['Contents'])
	return {"Contents" : result}

def cache_exists(this_observer_uuid, cache_name="quick_access_cache"):
	try:
		s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/{cache_name}.json").get()['Body'].read()
		return True
	except:
		return False

def cache_read(this_observer_uuid, cache_name="quick_access_cache", template_quick_access_cache=template_quick_access_cache):
	try:
		return json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/{cache_name}.json").get()['Body'].read())
	except:
		return template_quick_access_cache

def cache_write(this_observer_uuid, quick_access_cache=template_quick_access_cache, cache_name="quick_access_cache"):
	s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/{cache_name}.json').put(Body=json.dumps(quick_access_cache, indent=3))

def ping(event, context, request_body, response_body):
	print("Ping!")
	return response_body

def overwrite(event, context, request_body, response_body):
	s3.Object("fta-mobile-observations-overwrites",str(int(time.time()))).put(Body=json.dumps(request_body, indent=3))
	return response_body

def joined(event, context, request_body, response_body):
	s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS,
		f'{request_body["observerID"]}/joined_at.json').put(Body=json.dumps({"joined_at_raw" : request_body["joinedAt"], "system_information" : json.loads(request_body["systemInformation"])}))


def process_data_donation_v3(event, context, request_body, response_body):
	try:
		ad_id = request_body["ad_id"]
		observer_id = request_body["observer_id"]
		filename = request_body["filename"]
		content = request_body["content"]
		filename_in_s3 = f"{observer_id}/temp-v2/{ad_id}/{filename}"
		is_json = (filename.endswith(".json"))
		content = content if (is_json) else base64.b64decode(content)
		s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS,filename_in_s3).put(Body=content)
		quick_access_cache = cache_read(observer_id, cache_name="quick_access_cache")
		quick_access_cache["observations"].append(f"{observer_id}/temp-v2/{ad_id}/")
		cache_write(observer_id, quick_access_cache, cache_name="quick_access_cache")
		# Add the result to the entry-point cache
		if (is_json):
			content_loaded = json.loads(content)
			observed_at = int(content_loaded["nameValuePairs"]["observedAt"])
			prepared_at = int(content_loaded["nameValuePairs"]["preparedAt"])
			fps = float(content_loaded["nameValuePairs"]["recordingInformation"]["internalJSONObject"]["nameValuePairs"]["FPS"])
			frames = {k:(observed_at + (int(k)/fps)) for k in content_loaded["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"].keys()}
			# If this is the first-run, we can expect that the entrypoint_cache will be empty, in which case we are going to
			# create the entire entrypoint_cache
			entrypoint_cache = cache_read(observer_id, "entrypoint_cache", dict())
			if (len(entrypoint_cache.keys()) == 0):
				print("Generating cache for ", observer_id)
				entrypoint_cache = init_data_donation_organizer_cache(observer_id)
			template_entrypoint_data_donation["observed_at"] = observed_at
			template_entrypoint_data_donation["prepared_at"] = prepared_at
			template_entrypoint_data_donation["received_at"] = int(time.time())
			template_entrypoint_data_donation["frames"] = frames
			entrypoint_cache[ad_id] = template_entrypoint_data_donation
			cache_write(observer_id, entrypoint_cache, cache_name="entrypoint_cache")
		response_body["dispatched"] = "TRUE"
	except:
		print(traceback.format_exc())
		response_body["dispatched"] = "FALSE"
	return response_body

def process_log(event, context, request_body, response_body):
	try:
		this_observer_uuid = request_body["observer_id"]
		content_decoded = base64.b64decode(request_body["content"])
		s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/logs/{int(time.time())}.json').put(Body=content_decoded)
		response_body["dispatched"] = "TRUE"
	except:
		print(traceback.format_exc())
		response_body["dispatched"] = "FALSE"
	return response_body




'''
	This function determines the contents of a subbucket within an S3 bucket
'''
def subbucket_contents(kwargs, search_criteria="CommonPrefixes"):
	results = list()
	if (search_criteria == "CommonPrefixes"):
		results = [x for x in [prefix if (prefix is None) else prefix.get("Prefix") 
			for prefix in s3_client.get_paginator("list_objects_v2").paginate(
				**{**{"Delimiter" : "/"}, **kwargs}).search("CommonPrefixes")] if (x is not None)]
	else:
		try:
			for batch_obj in [x for x in s3_client.get_paginator("list_objects_v2").paginate(**{**{"Delimiter" : "/"}, **kwargs})]:
				for key_obj in batch_obj["Contents"]:
					results.append(key_obj["Key"])
		except:
			pass
			# Bucket is probably empty
	return results

def init_data_donation_organizer_cache(this_observer_uuid):
	entrypoint_cache = cache_read(this_observer_uuid, "entrypoint_cache", dict())
	# Examine the entire sub-bucket and return all enclosed directories
	this_subbucket_contents = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, "Prefix" : f"{this_observer_uuid}/temp-v2/"})
	# For any data donations that are not within the entrypoint_cache, instantiate them
	uncaptured = [y for y in [x.split("/")[-2] for x in this_subbucket_contents] if (not y in entrypoint_cache)]
	# Instantiate the data donations for which we don't have results
	print("Captured:", len(entrypoint_cache.keys()), "Uncaptured:", len(uncaptured))
	for y in uncaptured:
		print("\tAdding uncaptured data donation:", y)
		entrypoint_cache[y] = template_entrypoint_data_donation
	cache_write(this_observer_uuid, entrypoint_cache, "entrypoint_cache")
	return entrypoint_cache

processes = {
	"DATA_DONATION_V3" : process_data_donation_v3,
	"LOG" : process_log,
	"PING" : ping,
	"JOINED" : joined,
	"OVERWRITE" : overwrite
}

def lambda_handler(event, context):
	request_body = dict()
	response_body = dict()
	try:
		request_body = json.loads(event["body"])
	except:
		try:
			request_body = event
		except:
			pass
		pass

	# Evaluate the action
	if ("action" in request_body):
		if (request_body["action"] in processes):
			print("Action: ", request_body["action"])
			response_body = processes[request_body["action"]](event, context, request_body, response_body)

	return {
		'statusCode': 200,
		'body': json.dumps(response_body)
	}

def test_event_1():
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		alts = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, "Prefix" : this_observer_uuid+"/"})
		#print(alts)
		if ((this_observer_uuid+"/temp-v2/") in alts):
			entrypoint_cache = cache_read(this_observer_uuid, "entrypoint_cache", dict())
			if (len(entrypoint_cache.keys()) == 0):
				entrypoint_cache = init_data_donation_organizer_cache(this_observer_uuid)
			#cache_write(this_observer_uuid, entrypoint_cache, "entrypoint_cache")


if (__name__ == "__main__"):
	#entrypoint_cache = cache_read("OBSERVER_UUID_GOES_HERE", "entrypoint_cache", dict())
	#ipdb.set_trace()
	#test_event_1()
	#init_data_donation_organizer_cache("OBSERVER_UUID_GOES_HERE")
	pass











