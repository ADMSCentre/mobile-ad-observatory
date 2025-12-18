import sys
import os
if (__name__ == "__main__"):
	import ipdb
import random
import json
from io import BytesIO
import time
import json
import boto3
import botocore
import traceback
import base64
import requests
from scrape_meta_adlibrary import *
from sliding_levenshtein import *
from html_formatting import *
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError
from distributed_cache import *

lambda_client = boto3.client("lambda", config=botocore.config.Config(
										retries={'max_attempts': 0}, 
										read_timeout=840, 
										connect_timeout=600, 
										region_name="ap-southeast-2"))

AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"

SCRAPE_THRESHOLD_INTERVAL = 3 * 24 * 60 * 60

VERBOSE = True

def chunks_of_n_size(l, n):
	return [l[i:i+n] for i in range(int(),len(l),n)]

'''
	This function determines the contents of a subbucket within an S3 bucket
'''
def subbucket_contents(kwargs, search_criteria="CommonPrefixes"):
	results = list()
	if (search_criteria == "CommonPrefixes"):
		results = [x for x in [prefix if (prefix is None) else prefix.get("Prefix") 
			for prefix in AWS_CLIENT['s3'].get_paginator("list_objects_v2").paginate(
				**{**{"Delimiter" : "/"}, **kwargs}).search("CommonPrefixes")] if (x is not None)]
	else:
		try:
			for batch_obj in [x for x in AWS_CLIENT['s3'].get_paginator("list_objects_v2").paginate(**{**{"Delimiter" : "/"}, **kwargs})]:
				for key_obj in batch_obj["Contents"]:
					results.append(key_obj["Key"])
		except:
			pass
			# Bucket is probably empty
	return results


def s3_object_exists(this_bucket, this_path):
	try:
		AWS_CLIENT['s3'].head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False

def cache_exists(this_observer_uuid, cache_name="quick_access_cache"):
	try:
		AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/{cache_name}.json").get()['Body'].read()
		return True
	except:
		return False

def cache_read(this_observer_uuid, cache_name="quick_access_cache", template_cache=None):
	try:
		return json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/{cache_name}.json").get()['Body'].read())
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			if (VERBOSE): print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
			raise Exception()
		elif error_code == 'NoSuchKey':
			if (VERBOSE): print(f"Could not find cache '{cache_name}' for observer UUID: {this_observer_uuid}")
			return dict(template_cache)
		else:
			if (VERBOSE): print(f"❌ Other S3 error: {e}")
	except:
		return dict(template_cache)

def cache_write(this_observer_uuid, quick_access_cache, cache_name="quick_access_cache"):
	try:
		AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/{cache_name}.json').put(Body=json.dumps(quick_access_cache, indent=3))
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			if (VERBOSE): print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
		else:
			if (VERBOSE): print(f"❌ Other S3 error: {e}")
		raise Exception()
	except:
		if (VERBOSE): print(traceback.format_exc())
		raise Exception()
		pass


'''
def routine_instance_advertiser_name_extraction(rdo_obj):
	try:
		this_observer_uuid = rdo_obj["this_observer_uuid"]
		entry = rdo_obj["entry"]
		rdo_uuid = rdo_obj["rdo_uuid"]
		rdo_uuid_unsplit = rdo_obj["rdo_uuid_unsplit"]
		print(f"Executing on RDO: {rdo_uuid_unsplit}")
		# Take the entry and locate its OCR content
		this_rdo = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, entry).get()['Body'].read())
		ocr_data_reduced = list()
		[[ocr_data_reduced.append({k:y[k] for k in ["text", "confidence"]}) for y in x["ocr_data"]] for x in this_rdo["observation"]["keyframes"]]
		gpt_result = gpt_extract_advertiser_name(ocr_data_reduced)
		gpt_result["platform"] = this_rdo["observation"]["platform"]
		# Apply the tentative result
		AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
			f'{this_observer_uuid}/ccl/{rdo_uuid_unsplit}/advertiser_name_extraction.json').put(Body=json.dumps(gpt_result, indent=3))
		# Update the cache
		formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
		formalized_cache[rdo_uuid]["ccl_advertiser_name_extraction"] = int(time.time())
		cache_write(this_observer_uuid, formalized_cache, cache_name="formalized_cache")
	except:
		print(traceback.format_exc())
'''
'''
def advertiser_name_extraction_as_prompts(this_advertiser_name_extraction, done_correctly=True):
	uncollapsed_prompts = [{"query" : this_advertiser_name_extraction["result"][i]['advertiser_name'].lower(), "_i" : i, "aggregates" : [i]}
								for i in range(len(this_advertiser_name_extraction['result']))]
	collapsed_prompts = list()
	for i in range(len(uncollapsed_prompts)):
		found = False
		for j in range(len(collapsed_prompts)):
			if (done_correctly):
				if (levenshtein(uncollapsed_prompts[i]["query"],collapsed_prompts[j]["query"]) < MAX_LEVENSHTEIN_DISTANCE):
					found = True
					collapsed_prompts[j]["aggregates"].append(uncollapsed_prompts[i]['_i'])
					break
			else:
				if (levenshtein(uncollapsed_prompts[i]["query"],collapsed_prompts[j]["query"])):
					found = True
					collapsed_prompts[j]["aggregates"].append(uncollapsed_prompts[i]['_i'])
					break
		if (not found):
			collapsed_prompts.append(uncollapsed_prompts[i])
	return collapsed_prompts
'''
'''
def routine_instance_advertiser_scrape_complete(rdo_obj, scrape_instances_path, ii, alias=None):
	# Cannot be certain of when it might've been updated - load up now
	advertiser_prompts_updated = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
																scrape_instances_path).get()['Body'].read())
	advertiser_prompts_updated[ii]["complete"] = True
	if (alias is not None):
		advertiser_prompts_updated[ii]["alias"] = alias
	AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, scrape_instances_path).put(
												Body=json.dumps(advertiser_prompts_updated, indent=3))
	# If all scrapes are complete, flag it back to the formalized cache
	if any([(all([((y in x) and x[y]) for x in advertiser_prompts_updated])) for y in ["complete", "completed"]]):
		print("All scrape instances for this RDO are complete - flagging back to formalized cache")
		formalized_cache = cache_read(rdo_obj["this_observer_uuid"], cache_name="formalized_cache")
		formalized_cache[rdo_obj["rdo_uuid"]]["ccl_advertiser_scrape"] = int(time.time())
		cache_write(rdo_obj["this_observer_uuid"], formalized_cache, cache_name="formalized_cache")
'''
'''
def routine_instance_advertiser_scrape(rdo_obj):
	print(rdo_obj)
	# Load in the advertiser name extraction
	ccl_path = f'{rdo_obj["this_observer_uuid"]}/ccl/{rdo_obj["rdo_uuid_unsplit"]}'
	this_advertiser_name_extraction = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
		f'{ccl_path}/advertiser_name_extraction.json').get()['Body'].read())
	scrape_instances_path = f'{ccl_path}/scrape_instances.json'
	advertiser_prompts = None
	# If the scrape instances have not been designated yet...
	if (not s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, scrape_instances_path)):
		# Isolate the advertiser names as queries
		advertiser_prompts = advertiser_name_extraction_as_prompts(this_advertiser_name_extraction)
		# Populate each query with a completion status, and a unique identifier that will identify the scrape instance
		# associated with it
		for i in range(len(advertiser_prompts)):
			advertiser_prompts[i]["complete"] = False
			advertiser_prompts[i]["scrape_instance_uuid"] = str(uuid.uuid4())
		AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, scrape_instances_path).put(
				Body=json.dumps(advertiser_prompts, indent=3))
	else:
		advertiser_prompts = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
																scrape_instances_path).get()['Body'].read())
	# Go through each scrape instance, and call the scrape process if necessary
	this_vendor = None
	if (this_advertiser_name_extraction["platform"] in GLOBALS_CONFIG["platform_vendor_mappings"]):
		this_vendor = GLOBALS_CONFIG["platform_vendor_mappings"][this_advertiser_name_extraction["platform"]]
	contemporary_scrape_log_init()
	contemporary_scrape_log_path = f'moat_ccl_contemporary_scrape_log.json'
	contemporary_scrape_log = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-holding-bucket", 
															contemporary_scrape_log_path).get()['Body'].read())

	if (this_vendor is None): # Starting here
		for i in range(len(advertiser_prompts)):
			print("Passing due to absent scrape identity")
			# This can be resumed in future
			routine_instance_advertiser_scrape_complete(rdo_obj, scrape_instances_path, i)
	else:
		for i in range(len(advertiser_prompts)):
			# 
			# If this index is incomplete, we need to run the scraper and mass downloader
			#
			# Both parts are checked, in order, and the necessary events are called to take the scrape to success

			if (not (any([((x in advertiser_prompts[i]) and (advertiser_prompts[i][x])) for x in ["complete", "completed"]]))):
				scrape_output_path = f'{ccl_path}/{advertiser_prompts[i]["scrape_instance_uuid"]}/scrape_output.json'
				scrape_output_exists = s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, scrape_output_path)
				# If there exists a recent scrape that matches the prompt and vendor, set this scrape to alias backwards
				#
				# Note: Aliases are never revisited, as they flag the completion - furthermore, non-aliases never
				# feed into aliases, as we check the existence of a scrape output before allowing an alias to materialize
				alias = None
				if (not scrape_output_exists):
					for j in range(len(contemporary_scrape_log)):
						if ((this_vendor == contemporary_scrape_log[j]["vendor"])
							and (levenshtein(advertiser_prompts[i]["query"], contemporary_scrape_log[j]["query"]) <= MAX_LEVENSHTEIN_DISTANCE)):
							alias = contemporary_scrape_log[j]
							break
				if ((alias is not None) and ("scrape_instance_uuid" in alias) and (alias["scrape_instance_uuid"] is not None)):
					# Complete the process early, applying an alias
					#
					# Note: Aliases are not recorded in the contemporary scrape log
					print("Applying alias...")
					routine_instance_advertiser_scrape_complete(rdo_obj, scrape_instances_path, i, alias=alias)
				else:
					# Call the scraper (if possible)
					this_scrape_output_outlinks = None
					if (not scrape_output_exists):
						this_scrape_identity = get_available_scrape_identity(platform=this_advertiser_name_extraction["platform"])
						if (this_scrape_identity is None):
							print("Passing due to absent scrape identity")
							# This can be resumed in future
							routine_instance_advertiser_scrape_complete(rdo_obj, scrape_instances_path, i)
						else:
							if (not s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, scrape_output_path)):
								if (this_vendor == "meta_adlibrary"):
									this_scrape_output = routine_meta_adlibrary_scrape(this_scrape_identity, 
										query={ "queryString" : json.dumps(advertiser_prompts[i]["query"]) }, up_to=int(time.time()))
									AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, scrape_output_path).put(
											Body=json.dumps(this_scrape_output, indent=3))
									this_scrape_output_outlinks = this_scrape_output["response_interpreted"]["outlinks"]
									scrape_output_exists = True
									# Add the scrape to the contemporary log
									contemporary_scrape_log = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-holding-bucket", 
																			contemporary_scrape_log_path).get()['Body'].read())
									contemporary_scrape_log.append({
											"this_observer_uuid" : rdo_obj["this_observer_uuid"],
											"rdo_uuid" : rdo_obj["rdo_uuid"],
											"rdo_uuid_unsplit" : rdo_obj["rdo_uuid_unsplit"],
											"query" : advertiser_prompts[i]["query"],
											"vendor" : this_vendor,
											"timestamp" : int(time.time()),
											"scrape_instance_uuid" : advertiser_prompts[i]["scrape_instance_uuid"]
										})
									AWS_RESOURCE['s3'].Object("fta-mobile-observations-holding-bucket", contemporary_scrape_log_path).put(
																			Body=json.dumps(contemporary_scrape_log, indent=3))
								else:
									print("Passing due to absent scrape routine")
					# If there is a scrape_output - instantaneous in most cases
					if (scrape_output_exists):
						# Load in the outlinks if necessary
						if (this_scrape_output_outlinks is None):
							this_scrape_output_outlinks = json.loads(AWS_RESOURCE['s3'].Object(
									S3_BUCKET_MOBILE_OBSERVATIONS, scrape_output_path
								).get()['Body'].read())["response_interpreted"]["outlinks"]
						# Between the time that it was last loaded and now, it might've been updated
						advertiser_prompts_updated = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
																				scrape_instances_path).get()['Body'].read())
						# If the outlinks are empty, we can divert to early completion...
						if (len(this_scrape_output_outlinks) == 0):
							routine_instance_advertiser_scrape_complete(rdo_obj, scrape_instances_path, i)
						else:
							# Run a check on the 'mass-downloader' metadata - if the donwloads are complete, we can flag the event as complete
							metadata_path = f'{ccl_path}/{advertiser_prompts[i]["scrape_instance_uuid"]}/medias/metadata.json'
							if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, metadata_path)):
								metadata_obj = json.loads(AWS_RESOURCE['s3'].Object(
									S3_BUCKET_MOBILE_OBSERVATIONS, metadata_path).get()['Body'].read())
								if (len(this_scrape_output_outlinks) == len(metadata_obj["download_log"])):
									print("This scrape instance is complete - adjusting in scrape_instances object")
									# Run completion of scrape instance
									routine_instance_advertiser_scrape_complete(rdo_obj, scrape_instances_path, i)
								else:
									print("This scrape was called prior, and did not complete the 'mass download' aspect correctly")
									print("It may still be running...")
									raise Exception(metadata_path)
							else:
								print("Calling mass downloader...")
								# Call the mass downloader (chunked and asynchronous)
								#
								# Note: The most calls that can be downloaded in a 'mass-download' execution are roughly 200KB worth.
								# We will assume that 200 links per chunk fits within this specification. Then, we chunk up the download
								# into said parts, and set the indication back here that when the associated metadata reads that
								# the n_to_download have been retrieved, we can flag that the scrape is finally complete.
								lambda_client = boto3.client("lambda", config=botocore.config.Config(
																				retries={'max_attempts': 0}, 
																				read_timeout=840, 
																				connect_timeout=600, 
																				region_name="ap-southeast-2"))
								MAX_N_TO_DOWNLOAD = 200
								chunks = chunks_of_n_size([{ "url" : x } for x in this_scrape_output_outlinks], MAX_N_TO_DOWNLOAD)
								for this_chunk in chunks:
									invoke_response = lambda_client.invoke(
										FunctionName="arn:aws:lambda:ap-southeast-2:519969025508:function:moat_downloader", 
										InvocationType="Event", 
										Payload=json.dumps({ 
											"action": "download",
											"vendor" : this_vendor,
											"output_location" : f'{ccl_path}/{advertiser_prompts[i]["scrape_instance_uuid"]}/medias',
											"content_to_download" : this_chunk
										}))
'''







'''
	
	create an outlet for the proxies infrastructure
	
	the proxies (and their associated identities) need to be linked into the ad scrape infrastructure
	as a pool of available identities - this works as follows

		scraper has a request query for some given platform
			
		pool of identities should be held in online location - with capacity to be managed locally

			i.e. make scrape_meta_adlibrary object in fta-mobile-observations-holding-bucket horizontal

			add on a last scrapes object - depicts all scrapes within last hour

				two scrapes cannot be less than 2-6 minutes apart

				no more than 20 scrapes can happen in an hour

	
		pool of identities is assessed to determine if an identity exists that is...

			for said platform

			is unblocked

			was not called recently 






	we need to firstly know what identities we have to work with

	then we have to know how to insert custom start dates for ads

		see test

	then we need a routine that calls ad scraping carefully



'''







'''
	Batch event
'''
'''
N_SECONDS_TIMEOUT = (60 * 10)
N_DATA_DONATIONS_TO_PROCESS_WITHIN_BATCH = 3000
def routine_batch_advertiser_name_extraction(event, context=None):
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	rdos_for_ccl_scrape = list()
	# For each observer, if the entrypoint_cache exists, load in the data donations that need OCR
	#take rdos that are complete
	#take ocrs
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.split("/")[0]
		print(this_observer_uuid)
		formalized_cache_path = f"{this_observer_uuid}/formalized_cache.json"
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, formalized_cache_path)):
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
			if ("Contents" in response):
				for x in response["Contents"]:
					if ("output.json" in x["Key"]):
						rdo_uuid_unsplit = x["Key"].split("/")[-2]
						rdo_uuid = rdo_uuid_unsplit.split(".")[1]
						if ((rdo_uuid in formalized_cache) and ("rdo" in formalized_cache[rdo_uuid]) 
								and (not "ccl_advertiser_name_extraction" in formalized_cache[rdo_uuid])):
							rdos_for_ccl_scrape.append({
									"this_observer_uuid" : this_observer_uuid,
									"entry" : x["Key"],
									"rdo_uuid" : rdo_uuid, 
									"rdo_uuid_unsplit" : rdo_uuid_unsplit
								})

	# Shuffle the ads for randomness
	random.shuffle(rdos_for_ccl_scrape)

	# Take n and process (we attempt to process as many as we can - if the execution drops off, we can always pick
	# it up later, although separating the execution into separate sub-instance style lambdas would be more computationally
	# expensive, as we would need to reload the easyocr module each time - this is why we try to do it all in one hit)
	for entry in rdos_for_ccl_scrape[:N_DATA_DONATIONS_TO_PROCESS_WITHIN_BATCH]:
		elapsed_time = abs(int(time.time()) - time_at_init)
		if (elapsed_time < N_SECONDS_TIMEOUT): 
			# Set the timeout for 5 minutes, although the real timeout is technically 6 minutes - this allows
			# a comfortable drop-off
			routine_instance_advertiser_name_extraction(entry)
			pass
		else:
			print("Calling early exit on timeout...")
			break
	return str()
'''
'''
def routine_batch_advertiser_scrape(event, context=None):
	time_at_init = int(time.time())

	# Clean the contemporary_scrape_log, once per batch job
	contemporary_scrape_log_path = f'moat_ccl_contemporary_scrape_log.json'
	contemporary_scrape_log_init()
	contemporary_scrape_log = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-holding-bucket", 
															contemporary_scrape_log_path).get()['Body'].read())
	# Clear old entries
	contemporary_scrape_log = [x for x in contemporary_scrape_log if (x["timestamp"] >= (int(time.time()) - SCRAPE_THRESHOLD_INTERVAL))]
	# Sort from old to new
	contemporary_scrape_log = sorted(contemporary_scrape_log, key=lambda d: d["timestamp"])
	# Commit
	AWS_RESOURCE['s3'].Object("fta-mobile-observations-holding-bucket", contemporary_scrape_log_path).put(
																	Body=json.dumps(contemporary_scrape_log, indent=3))
	##############

	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	tentative_ccl_scrape = list()
	# For each observer, if the entrypoint_cache exists, load in the data donations that need OCR
	#take rdos that are complete
	#take ocrs
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.split("/")[0]
		print(this_observer_uuid)
		formalized_cache_path = f"{this_observer_uuid}/formalized_cache.json"
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, formalized_cache_path)):
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
			if ("Contents" in response):
				for x in response["Contents"]: # TODO
					if ("output.json" in x["Key"]):
						rdo_uuid_unsplit = x["Key"].split("/")[-2]
						rdo_uuid = rdo_uuid_unsplit.split(".")[1]
						if ((rdo_uuid in formalized_cache) and ("rdo" in formalized_cache[rdo_uuid]) 
								and (not "ccl_advertiser_scrape" in formalized_cache[rdo_uuid])):
							tentative_ccl_scrape.append({
									"this_observer_uuid" : this_observer_uuid,
									"entry" : x["Key"],
									"rdo_uuid" : rdo_uuid, 
									"rdo_uuid_unsplit" : rdo_uuid_unsplit
								})

	# Shuffle the ads for randomness
	random.shuffle(tentative_ccl_scrape)

	# Take n and process (we attempt to process as many as we can - if the execution drops off, we can always pick
	# it up later, although separating the execution into separate sub-instance style lambdas would be more computationally
	# expensive, as we would need to reload the easyocr module each time - this is why we try to do it all in one hit)
	for entry in tentative_ccl_scrape[:N_DATA_DONATIONS_TO_PROCESS_WITHIN_BATCH]:
		elapsed_time = abs(int(time.time()) - time_at_init)
		if (elapsed_time < N_SECONDS_TIMEOUT): 
			# Set the timeout for 5 minutes, although the real timeout is technically 6 minutes - this allows
			# a comfortable drop-off
			routine_instance_advertiser_scrape(entry)
			pass
		else:
			print("Calling early exit on timeout...")
			break

	#TODO ******************
	#
	#as a temporary measure, make it such that anything in the original enrichment scrape is not included
	#this is achieved by loading hte original enrichment into the system (refer to the general process to get
	#an idea of what to synthesize)
	#
	#	make it such that only content after tuesday can be scraped (an acutal solution)
	return str()
'''

'''
	go through each cache and delete the contents
'''
def repair():
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	rdos_for_ccl_scrape = list()
	# For each observer, if the entrypoint_cache exists, load in the data donations that need OCR
	'''
		take rdos that are complete

		take ocrs
	'''
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.split("/")[0]
		if (VERBOSE): print(this_observer_uuid)
		formalized_cache_path = f"{this_observer_uuid}/formalized_cache.json"
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, formalized_cache_path)):
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			for k in formalized_cache:
				try: 
					del formalized_cache[k]["ccl_advertiser_name_extraction"]
				except: pass
			cache_write(this_observer_uuid, formalized_cache, cache_name="formalized_cache")
			response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/ccl/")
			if ("Contents" in response):
				for x in response["Contents"]:
					# Delete ...
					if (VERBOSE): print("Deleting file...", x["Key"])
					AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, x["Key"]).delete()



'''
	This diagnostic determines the amount of the full data donation database that is currently indexed with records in the ccl_cache

	It works by firstly taking a measure of all formalized data donations (which is what technically matters, as the latter are FPs)
	and then checking the portion therein that have been covered by our own ccl_cache

	As the ccl_cache technically works with data donations, they are firstly mapped back to their formalized UUIDs
	Then those figures are covered over the formalized UUIDs of the entire dataset.
'''
def diagnostic_ccl_cache_coverage():
	# Complete dataset is firstly received
	observers_to_formalized_uuids = dict()
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	formalized_caches = dict()
	entrypoint_caches = dict()
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.split("/")[0]
		if (VERBOSE): print(this_observer_uuid)
		observers_to_formalized_uuids[this_observer_uuid] = list()
		formalized_cache_path = f"{this_observer_uuid}/formalized_cache.json"
		entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
		if ((s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, formalized_cache_path))
			and (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path))):
			entrypoint_caches[this_observer_uuid] = cache_read(this_observer_uuid, cache_name="entrypoint_cache")
			formalized_caches[this_observer_uuid] = cache_read(this_observer_uuid, cache_name="formalized_cache")
			observers_to_formalized_uuids[this_observer_uuid].extend([k for k in formalized_caches[this_observer_uuid]])
			observers_to_formalized_uuids[this_observer_uuid] = list(set(observers_to_formalized_uuids[this_observer_uuid]))
	# Then the CCL cache is indexed
	ccl_cache = distributed_cache_read({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_cache_distributed"
			}
		})

	# As is the data donatoin cache
	#ccl_data_donation_cache = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
	#														"ccl_data_donation_cache.json").get()['Body'].read())
	ccl_data_donation_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				}
			})
	# Map every term_uuid to its data donations, before consequently mapping them to the formalized UUIDs
	observers_to_formalized_uuids_from_ccl = dict()
	disinclusions = list()
	for term_uuid in ccl_cache:
		unsplit_data_donation_uuids = [x for x in ccl_data_donation_cache if (term_uuid in ccl_data_donation_cache[x]["group_term_uuids"])]
		for this_unsplit_data_donation_uuid in unsplit_data_donation_uuids:
			this_observer_uuid, data_donation_uuid = this_unsplit_data_donation_uuid.split("/")
			if (this_observer_uuid in entrypoint_caches):
				this_entry = entrypoint_caches[this_observer_uuid][data_donation_uuid.replace(".json", str())]
				if ("formalized_v2_uuids" in this_entry):
					formalized_uuids = this_entry["formalized_v2_uuids"]
					if (not this_observer_uuid in observers_to_formalized_uuids_from_ccl):
						observers_to_formalized_uuids_from_ccl[this_observer_uuid] = list()
					observers_to_formalized_uuids_from_ccl[this_observer_uuid].extend(formalized_uuids)
					observers_to_formalized_uuids_from_ccl[this_observer_uuid] = list(set(observers_to_formalized_uuids_from_ccl[this_observer_uuid]))
			else:
				if (VERBOSE): print("disinclude: "+this_observer_uuid)
				disinclusions.append(this_observer_uuid)
	with open("disinclusions.json", "w") as f: 
		f.write(json.dumps(disinclusions,indent=3))
	with open("observers_to_formalized_uuids.json", "w") as f: 
		f.write(json.dumps(observers_to_formalized_uuids,indent=3))
	with open("observers_to_formalized_uuids_from_ccl.json", "w") as f: 
		f.write(json.dumps(observers_to_formalized_uuids_from_ccl,indent=3))

	ipdb.set_trace()

def diagnostic_ccl_cache_coverage_stats():
	observers_to_formalized_uuids = json.loads(open("observers_to_formalized_uuids.json").read())
	observers_to_formalized_uuids_from_ccl = json.loads(open("observers_to_formalized_uuids_from_ccl.json").read())
	# Compare
	statistics = {x:list() for x in ["accounted", "unaccounted"]}
	for this_observer_uuid in observers_to_formalized_uuids:
		for this_formalized_uuid in observers_to_formalized_uuids[this_observer_uuid]:
			accounted = ((this_observer_uuid in observers_to_formalized_uuids_from_ccl) 
				and (this_formalized_uuid in observers_to_formalized_uuids_from_ccl[this_observer_uuid]))
			this_record = f"{this_observer_uuid}/{this_formalized_uuid}"
			if (accounted):
				statistics["accounted"].append(this_record)
			else:
				statistics["unaccounted"].append(this_record)
	ipdb.set_trace()


def advertiser_scrape_v2_complete(this_item, outcome_appendage):
	'''
		Note: Consider that multiple data donations are associated with a single formalized UUID.
		While it would be wise to recompile once at the end of all data donations being completed, this code block
		implements an 'eager' strategy to recompile instead at the completion of each individual data donation,
		which in turn reflects scrape data immediately.
	'''
	this_item["outcome"] = {"at" : int(time.time()), "version" : GLOBALS_CONFIG["version"]} | outcome_appendage
	# The formalized cache is then adjusted to reflect the changes - the RDO process will
	# pick this up at a later stage.
	# 
	# Load in the grouped_terms
	this_grouped_terms = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
							f'grouped_terms/{this_item["observer_uuid"]}/{this_item["group_uuid"]}.json').get()['Body'].read())
	# Isolate the group that deals with this entry
	this_data_donation_uuids = [x["data_donation_uuid"] for x in this_grouped_terms[this_item["group_i"]]["members"]]
	# Retrieve the entrypoint cache
	this_entrypoint_cache = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2", 
							f'{this_item["observer_uuid"]}/entrypoint_cache.json').get()['Body'].read())
	# Retrieve the formalized UUIDs
	if (VERBOSE): print(this_data_donation_uuids)
	this_formalized_uuids = list(); 
	for x in this_data_donation_uuids:
		if ("formalized_v2_uuids" in this_entrypoint_cache[x]):
			this_formalized_uuids.extend(this_entrypoint_cache[x]["formalized_v2_uuids"])
	this_formalized_uuids = list(set(this_formalized_uuids))
	# Go to each formalized UUIDs object within the formalized_cache and mark them accordingly

	# In some cases, a ccl scrape may be undertaken for a formalizer cache that doesn't exist (not sure what causes this)
	# We attempt to salvage here...
	try:
		formalized_cache = cache_read(this_item["observer_uuid"], cache_name="formalized_cache")
		for this_formalized_uuid in this_formalized_uuids:
			formalized_cache[this_formalized_uuid]["ccl_advertiser_scrape_v2"] = int(time.time())
		cache_write(this_item["observer_uuid"], formalized_cache, cache_name="formalized_cache")
	except:
		this_item["outcome"]["status"] += "_AND_FORMALIZER_ISSUE"
		this_item["outcome"]["formalizer_orphan_uuids"] = this_formalized_uuids

	# Also update the CCL cache to reflect this entry correctly - we need to read it in before making the update
	distributed_cache_write({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				},
				"longitudinal_unit" : A_DAY,
				"longitudinal_key" : ["timestamp"],
				"input" : { this_item["uuid"] : this_item }
			})
	if (VERBOSE): print("SCRAPE COMPLETED:")
	if (VERBOSE): print(json.dumps(this_item,indent=3))
	'''
	ccl_cache = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", "ccl_cache.json").get()['Body'].read())
	ccl_cache[this_item["uuid"]] = this_item
	try:
		AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", "ccl_cache.json").put(Body=json.dumps(ccl_cache, indent=3))
		print("SCRAPE COMPLETED:")
		print(json.dumps(this_item,indent=3))
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
		else:
			print(f"❌ Other S3 error: {e}")
		raise Exception()
	'''


N_SECONDS_TIMEOUT = (60 * 14)
CCL_TERM_LIKENESS_PCT = 0.85
CCL_TERM_TYPEAHEAD_LIKENESS_PCT = 0.75
LOOKBEHIND_INTERVAL_SECONDS = 60 * 60 * 24 * 3

def routine_batch_advertiser_scrape_v2(event, context, debug=False):
	time_at_init = int(time.time())
	'''
		Load in the entire cache
	'''
	'''
	ccl_cache = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
															"ccl_cache.json").get()['Body'].read())
	'''
	ccl_cache = distributed_cache_read({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_cache_distributed"
			}
		})
	ccl_cache_itemized = [v for v in list(ccl_cache.values())]
	ccl_cache_itemized = sorted(ccl_cache_itemized, key=lambda d: d["timestamp"])
	for this_item in ccl_cache_itemized:
		#print(json.dumps(this_item, indent=3))
		elapsed_time = abs(int(time.time()) - time_at_init)
		if (elapsed_time > N_SECONDS_TIMEOUT): 
			break
		# When a new scraper is added, we have to update the version, to reindex everything
		# 
		# Note: To avoid reindexing scrapes that are well-formed, we also overlook entries that are marked as 'SCRAPED'
		if (("outcome" in this_item) and ((this_item["outcome"]["version"] == GLOBALS_CONFIG["version"]) or (this_item["outcome"]["status"] == "SCRAPED"))):
			# If the item has an outcome with an 'up-to-date' version, we can overlook it
			if (debug): 
				if (VERBOSE): print("* Overlooking as outcome already exists")
		else:
			# Determine if we have a scraper for this item
			this_vendor = None
			if (this_item["platform"] in GLOBALS_CONFIG["platform_vendor_mappings"]):
				this_vendor = GLOBALS_CONFIG["platform_vendor_mappings"][this_item["platform"]]
			# If an item is determined to not have a scraper, we flag it as having been evaluated
			# and that we could not have scraped it... if we update the available scrapers in future,
			# we adjust the version of the CCL scraper to reflect this so that previously overlooked entries
			# can be rescraped
			if (this_vendor is None):
				advertiser_scrape_v2_complete(this_item, {"status" : "NO_SCRAPER_AVAILABLE"})
				if (debug): 
					if (VERBOSE): print("* Overlooking as no vendor exists")
			else:
				# If this block is reached, we have a vendor and can proceed...
				#
				# The next check is to determine that the term is well-formed - here we catch any final issues that might be related to the term
				if (this_item["term"].lower().endswith(" now")):
					advertiser_scrape_v2_complete(this_item, {"status" : "MALFORMED_TERM", "comment" : "TRAILING_SUBTERM_NOW"})
					if (debug): 
						if (VERBOSE): print("* Overlooking as TRAILING_SUBTERM_NOW")
				elif (this_item["term"].lower() in ["learn more", "comment", "like", "send", "shop", "reply", "like this page", "follow", "share"]):
					advertiser_scrape_v2_complete(this_item, {"status" : "MALFORMED_TERM", "comment" : "GENERIC_AD_TERM"})
					if (debug): 
						if (VERBOSE): print("* Overlooking as GENERIC_AD_TERM")
				elif ((len(this_item["term"]) <= 3) and (not (this_item["term"].upper() == this_item["term"]))):
					advertiser_scrape_v2_complete(this_item, {"status" : "MALFORMED_TERM", "comment" : "INSUFFICIENT_TERM_STRING_LENGTH"})
					if (debug): 
						if (VERBOSE): print("* Overlooking as INSUFFICIENT_TERM_STRING_LENGTH")
				else:
					# If the term passes the 'malformed term' checks, we can proceed with the scrape
					#
					# The next check assesses if a recent scrape was undertaken for the same term (or a very similar term ie., ~90% similarity)
					#
					# We go back a given time interval and determine whether there are any entries within that window that are:
					# 	* 'complete' (in the original sense ie., not also aliasing)
					# 	* Whether they also match the query term that was used here
					# 	* whether they correspond to the same platform (we relax the ad type criteria)
					lookbehind_to = this_item["timestamp"] - LOOKBEHIND_INTERVAL_SECONDS
					# Filtering by completion (we typically want to alias to a scrape that actually completed...
					filtered_related_items = [x for x in ccl_cache_itemized if (("outcome" in x) and (x["outcome"]["status"] == "SCRAPED"))]
					# Filtering by time...
					filtered_related_items = [x for x in filtered_related_items if ((x["timestamp"] >= lookbehind_to) and (x["timestamp"] < this_item["timestamp"]))]
					# Filtering by platform...
					filtered_related_items = [x for x in filtered_related_items if (x["platform"] == this_item["platform"])]
					# Filtering by similar terms
					filtered_related_items = [x for x in filtered_related_items 
						if (sliding_levenshtein_pct(x["term"], this_item["term"], MIN_QUERY_STRING_LENGTH=3) >= CCL_TERM_LIKENESS_PCT)]
					# If after all filtrations, there is at least one related term, alias to that term
					if (len(filtered_related_items) > 0):
						advertiser_scrape_v2_complete(this_item, {"status" : "ALIASED", "alias_uuid" : filtered_related_items[0]["uuid"]})
						if (debug): 
							if (VERBOSE): print("* ALIASED")
					else:
						# In this case, proceed with the scrape
						#
						# Check that the scrapers for the vendor are available
						# 
						# Under normal circumstances, the get_available_scrape_identity function can block until a scraper is present - this works
						# in scenarios where a common vendor is used - however here multiple vendors are considered ie., one vendor might be available,
						# and the others not. So its wiser to just let the loop continue...
						#time.sleep(2)
						tentative_scrape_identity = get_available_scrape_identity(platform=this_item["platform"], divert_designation_block=True, assert_lock_check=True)
						if ((tentative_scrape_identity == "DIVERTED") or (tentative_scrape_identity is None)):
							# When no scraper is available, we can't do anything for this entry and so have to pass it
							# - note this is different from not having a 'vendor scraper' available
							if (VERBOSE): print("Passing due to absent scrape identity")
							if (debug): 
								if (VERBOSE): print("* Overlooking for absent scraper issue")
						else:
							'''
								Suppose I have 30 proxies, where each can do up to 20 scrapes an hour

								30 * 20 = 600 potential scrapes can be undertaken for that hour

								HOWEVER

								if each scrape takes 10 seconds to complete, and the instance can only run for 15 minutes

								then the most scrapes that can be undertaken for that period are ((15 * 60) / 10) = 90

								as there are 4 calls in an hour (each for each 15 minutes), that makes 90 * 4 = 360 scrapes at max

								which is below our potential coverage

								to overcome this, each scrape needs to be undertaken on the second within a separate call of the instance
							'''
							if (this_vendor == "meta_adlibrary"):
								routine_batch_advertiser_scrape_instance_meta_ad_library({
										"this_item" : this_item
									}, None)

								'''
								print("Calling scrape instance for Meta Ad Library on item: ", this_item["uuid"])
								invoke_response = lambda_client.invoke(
									FunctionName=context.invoked_function_arn, InvocationType='Event', 
									Payload=json.dumps({ 
										"action": "advertiser_scrape_instance_meta_ad_library",
										"this_item" : this_item
									}))
								'''

								'''

								# The term is firstly cross-checked against the name-checker - we determine whether there is a match
								# for the term from the candidates that are returned, and intelligently assess instagram and Facebook
								# candidates therefrom
								this_namecheck_output = {
										"original_term" : this_item["term"], 
										"output" : routine_meta_adlibrary_namecheck(this_scrape_identity, this_item["term"])
									}
								designated_term = this_item["term"]
								if (not "error" in this_namecheck_output):
									# The namecheck has been undertaken, select the best most likely candidate, if it surpasses the threshold
									# Note: If we are dealing with Instagram
									try:
										# Run comparisons
										this_namecheck_output["comparisons"] = {"page_results" : list()}
										candidate_page_results = this_namecheck_output["output"]["data"]["ad_library_main"]["typeahead_suggestions"]["page_results"]
										for x in candidate_page_results:
											tentative_selected_term = (x["name"] if (this_item["platform"] == "FACEBOOK") else x["ig_username"])
											similarity_pct = (float() if (tentative_selected_term is None) 
												else sliding_levenshtein_pct(this_item["term"], tentative_selected_term, MIN_QUERY_STRING_LENGTH=3))
											this_namecheck_output["comparisons"]["page_results"].append({
													"tentative_selected_term" : tentative_selected_term,
													"similarity_pct" : similarity_pct
												})
										# Select a candidate if it meets the threshold
										highest_likeness_pct = None
										selected_i = None
										for i in range(len(this_namecheck_output["comparisons"]["page_results"])):
											this_pct = this_namecheck_output["comparisons"]["page_results"][i]["similarity_pct"]
											if (this_pct >= CCL_TERM_TYPEAHEAD_LIKENESS_PCT):
												if (highest_likeness_pct is None):
													highest_likeness_pct = this_pct
													selected_i = i
												elif (this_pct > highest_likeness_pct):
													highest_likeness_pct = this_pct
													selected_i = i
										if (selected_i is not None):
											designated_term = candidate_page_results[selected_i]["name"]
										# persist i and high likeness pct
										this_namecheck_output["selection"] = {
												"highest_likeness_pct" : highest_likeness_pct,
												"selected_i" : selected_i
											}
									except:
										print(traceback.format_exc())
										pass
								# Persist the namecheck output
								this_namecheck_output = this_namecheck_output | {"designated_term" : designated_term}
								AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
									f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_item["uuid"]}/namecheck_output.json').put(Body=json.dumps(this_namecheck_output, indent=3))

								# Get the scrape output
								this_scrape_output = routine_meta_adlibrary_scrape(this_scrape_identity, 
									query={ "queryString" : json.dumps(designated_term) }, up_to=int(this_item["timestamp"]))
								# Save the output to a given location
								AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
									f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_item["uuid"]}/scrape_output.json').put(Body=json.dumps(this_scrape_output, indent=3))
								# Mark the item
								# Note: The mass download happens on a separate process
								advertiser_scrape_v2_complete(this_item, {"status" : "SCRAPED", "mass_downloaded" : False})
								if (debug): 
									print("* SCRAPED")
								'''
							# Note: The mass-downloader operates separately on indication that the scrape has completed

'''
	This function carries out an instance-based scrape for the Meta Ad Library
'''
def routine_batch_advertiser_scrape_instance_meta_ad_library(event, context, debug=False):
	this_item = event["this_item"]
	'''

	Issue is that one async instance attempts to use the scraper identity - while its in use and not finished, the second comes about
	and attempts to use it also. What needs to happen instead is that as soon as the scraper identity is available, it locks onto
	the current instance, preventing any other async processes from accessing it while its in usage. Then at the end of the routine,
	it unlocks
	'''
	this_scrape_identity = get_available_scrape_identity(platform=this_item["platform"], assert_lock_check=True)
	if (this_scrape_identity is None):
		# Early exit due to unavailable scraper
		if (VERBOSE): print("Exiting due to an unavailable scraper")
		return str()
	scrape_identity_key = f"scrape_identities/{this_scrape_identity['uuid']}.json"
	json_s3_save_holding(scrape_identity_key, this_scrape_identity | {"locked" : True})
	# The term is firstly cross-checked against the name-checker - we determine whether there is a match
	# for the term from the candidates that are returned, and intelligently assess instagram and Facebook
	# candidates therefrom
	this_namecheck_output = {
			"original_term" : this_item["term"], 
			"output" : routine_meta_adlibrary_namecheck(this_scrape_identity, this_item["term"])
		}
	designated_term = this_item["term"]
	if (not "error" in this_namecheck_output):
		# The namecheck has been undertaken, select the best most likely candidate, if it surpasses the threshold
		# Note: If we are dealing with Instagram
		try:
			# Run comparisons
			this_namecheck_output["comparisons"] = {"page_results" : list()}
			candidate_page_results = this_namecheck_output["output"]["data"]["ad_library_main"]["typeahead_suggestions"]["page_results"]
			for x in candidate_page_results:
				tentative_selected_term = (x["name"] if (this_item["platform"] == "FACEBOOK") else x["ig_username"])
				similarity_pct = (float() if (tentative_selected_term is None) 
					else sliding_levenshtein_pct(this_item["term"], tentative_selected_term, MIN_QUERY_STRING_LENGTH=3))
				this_namecheck_output["comparisons"]["page_results"].append({
						"tentative_selected_term" : tentative_selected_term,
						"similarity_pct" : similarity_pct
					})
			# Select a candidate if it meets the threshold
			highest_likeness_pct = None
			selected_i = None
			for i in range(len(this_namecheck_output["comparisons"]["page_results"])):
				this_pct = this_namecheck_output["comparisons"]["page_results"][i]["similarity_pct"]
				if (this_pct >= CCL_TERM_TYPEAHEAD_LIKENESS_PCT):
					if (highest_likeness_pct is None):
						highest_likeness_pct = this_pct
						selected_i = i
					elif (this_pct > highest_likeness_pct):
						highest_likeness_pct = this_pct
						selected_i = i
			if (selected_i is not None):
				designated_term = candidate_page_results[selected_i]["name"]
			# persist i and high likeness pct
			this_namecheck_output["selection"] = {
					"highest_likeness_pct" : highest_likeness_pct,
					"selected_i" : selected_i
				}
		except:
			if (VERBOSE): print(traceback.format_exc())
			pass
	# Persist the namecheck output
	this_namecheck_output = this_namecheck_output | {"designated_term" : designated_term}
	AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
		f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_item["uuid"]}/namecheck_output.json').put(Body=json.dumps(this_namecheck_output, indent=3))

	# Get the scrape output
	try:
		this_scrape_output = routine_meta_adlibrary_scrape(this_scrape_identity, 
			query={ "queryString" : json.dumps(designated_term) }, up_to=int(this_item["timestamp"]))
		# Save the output to a given location
		AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
			f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_item["uuid"]}/scrape_output.json').put(Body=json.dumps(this_scrape_output, indent=3))
		# Mark the item
		# Note: The mass download happens on a separate process
		advertiser_scrape_v2_complete(this_item, {
				"status" : "SCRAPED", 
				"scrape_meta_adlibrary_success" : this_scrape_output["response_interpreted"]["success"]
			})
	except:
		if (VERBOSE): print(traceback.format_exc())
	json_s3_save_holding(scrape_identity_key, json_s3_load_holding(scrape_identity_key) | {"locked" : False})
	if (debug): 
		if (VERBOSE): print("* SCRAPED")


'''
	This function retrieves high-level stats for all scrapers

	It reports:

		* The status of all scrapers

		* When the last scrapes were conducted

		* When the last caches were undertaken

		* The coverage of the CCL cache
'''

from datetime import datetime
import pytz

def unix_to_datetime_string(timestamp: int, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
	if (timestamp is None): return "N/A"
	brisbane_tz = pytz.timezone("Australia/Brisbane")
	return datetime.fromtimestamp(timestamp, tz=brisbane_tz).strftime(fmt)

def routine_report_scrape_status(event, context):
	statistics = {"vendors" : dict()}
	scrape_identities = get_all_scrape_identities()
	html_aggregate_complete = str()
	for this_vendor_mapping in list(set(GLOBALS_CONFIG["platform_vendor_mappings"].values())):
		statistics["vendors"][this_vendor_mapping] = list()
		available_scrape_identities = [x for x in scrape_identities if (x["vendor"] == this_vendor_mapping.upper())]
		for x in available_scrape_identities:
			tentative_statistic = {k:x[k] for k in ["uuid", "valid", "created_at", "last_cached_at", "public_ip_address_meta_adlibrary"]}
			tentative_statistic["n_recent_scrapes"] = len(x["scrape_log"])
			tentative_statistic["locked"] = ("NULL" if (not "locked" in x) else x["locked"])
			tentative_statistic["last_scrape_at"] = None if (len(x["scrape_log"]) == 0) else max([y["timestamp"] for y in x["scrape_log"]])
			for z in ["last_scrape_at", "created_at", "last_cached_at"]:
				tentative_statistic[z] = unix_to_datetime_string(tentative_statistic[z] if (type(tentative_statistic[z]) in [int, type(None)]) else int(tentative_statistic[z]))
			statistics["vendors"][this_vendor_mapping].append(tentative_statistic)
		
		# Compile the HTML
		html_aggregate = f'<h2>Vendor: {titleize_key(this_vendor_mapping)}</h2>'
		for x in statistics["vendors"][this_vendor_mapping]: html_aggregate += dict_to_html_table(x) + "<br>"
		html_aggregate_complete += "<br>" + html_aggregate + "<br>"
	send_gmail("obei@qut.edu.au", "MOAT - Scraper Health Report", html_complete(html_aggregate_complete))



def datetime_test(elements, case="DAY"):
	from datetime import datetime, timedelta
	now = datetime.now()
	counts = {i: 0 for i in range(7)}
	if (VERBOSE): print(f"SCRAPE COMPLETIONS - LAST 7 {case}S:")
	if (case == "DAY"):
		for elem in elements:
			if ("outcome" in elem):
				ts = elem["outcome"]["at"]
				if ts is None:
					continue
				ts_dt = datetime.fromtimestamp(ts)
				diff_days = (now.date() - ts_dt.date()).days
				if 0 <= diff_days < 7:
					counts[diff_days] += 1
		for i in range(7):
			day_label = (now - timedelta(days=i)).strftime('%Y-%m-%d')
			if (VERBOSE): print(f"{day_label}: {counts[i]} elements")
	else:
		for elem in elements:
			if ("outcome" in elem):
				ts = elem["outcome"]["at"]
				if ts is None:
					continue
				
				ts_dt = datetime.fromtimestamp(ts)
				diff = now - ts_dt
				diff_hours = int(diff.total_seconds() // 3600)
				
				if 0 <= diff_hours < 7:
					counts[diff_hours] += 1
		# Print results from most recent to oldest
		for i in range(7):
			hour_label = (now - timedelta(hours=i)).strftime('%Y-%m-%d %H:00')
			if (VERBOSE): print(f"{hour_label}: {counts[i]} elements")

'''
	This function summarizes the CCL cache's coverage

	TODO - make an email for this
'''
def routine_report_ccl_cache_coverage(event, context):
	'''
	ccl_cache = json.loads(AWS_RESOURCE['s3'].Object("fta-mobile-observations-v2-ccl", 
															"ccl_cache.json").get()['Body'].read())
	'''
	ccl_cache = distributed_cache_read({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_cache_distributed"
			}
		})
	ccl_cache_itemized = [v for v in list(ccl_cache.values())]
	statistics = dict()
	for x in ccl_cache.values():
		if (not x["platform"] in statistics): statistics[x["platform"]] = {y:int() for y in  ["complete", "todo"]}
		if ("outcome" in x):
			statistics[x["platform"]]["complete"] += 1
		else:
			statistics[x["platform"]]["todo"] += 1
	if (VERBOSE): print(json.dumps(statistics,indent=3))
	datetime_test(ccl_cache_itemized, case="DAY")
	datetime_test(ccl_cache_itemized, case="HOUR")
	ipdb.set_trace()


processes = {
		"batch_advertiser_scrape_v2" : routine_batch_advertiser_scrape_v2,
		"report_scrape_status" : routine_report_scrape_status,
		"report_ccl_cache_coverage" : routine_report_ccl_cache_coverage,
		"advertiser_scrape_instance_meta_ad_library" : routine_batch_advertiser_scrape_instance_meta_ad_library
	}

#datetime_test(ccl_cache_itemized)

def lambda_handler(event, context):
	response_body = dict()

	# Evaluate the action
	if ("action" in event):
		if (event["action"] in processes):
			if (VERBOSE): print("Action: ", event["action"])
			response_body = processes[event["action"]](event, context)

	return {
		'statusCode': 200,
		'body': json.dumps(response_body)
	}



if (__name__ == "__main__"):
	#routine_batch_advertiser_scrape_v2(None, None, debug=True)
	routine_report_ccl_cache_coverage(None, None)
	routine_report_scrape_status(None, None)
	#routine_batch_advertiser_scrape_v2(debug=True)
	#diagnostic_ccl_cache_coverage_stats()#

