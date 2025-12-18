


import time
import sys
import os
import boto3
import uuid
import shutil
if (__name__ == "__main__"):
	import ipdb
import json
import random
import traceback
import requests
import botocore
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError
from collections import Counter
from distributed_cache import *
# TODO - local test functionality

PERMISSIBLE_OUTCOMES_FOR_COMPLETION = ["SUCCESS", "NO_OUTLINKS"]
S3_OBSERVATIONS_BUCKET = "fta-mobile-observations-v2"
S3_MOBILE_OBSERVATIONS_CCL_BUCKET = "fta-mobile-observations-v2-ccl"
CCL_CONFIG = {
		"platform_vendor_mappings" : {
			"FACEBOOK" : "meta_adlibrary",
			"INSTAGRAM" : "meta_adlibrary"
		},
		"aws" : {
			"AWS_PROFILE" : "dmrc",
			"AWS_REGION" : "ap-southeast-2"
		}
	}
MAX_EXECUTION_TIME = 60 * 10 # 10 minutes

##############################################################################################################################
##############################################################################################################################
### AWS
##############################################################################################################################
##############################################################################################################################

# Load up the necessary AWS infrastructure
# Note: On remote infrastructures, we don't authenticate as the Lambda handler will have the necessary
# permissions built into it
AWS_REQUIRED_RESOURCES = ["s3"]

def aws_load(running_locally=False):
	credentials_applied = dict()
	if (running_locally):
		# Running locally
		credentials = boto3.Session(profile_name=CCL_CONFIG["aws"]["AWS_PROFILE"]).get_credentials()
		credentials_applied = {
				"region_name" : CCL_CONFIG["aws"]["AWS_REGION"],
				"aws_access_key_id" : credentials.access_key,
				"aws_secret_access_key" : credentials.secret_key
			}
	AWS_RESOURCE = {k : boto3.resource(k, **credentials_applied) for k in AWS_REQUIRED_RESOURCES}
	AWS_CLIENT = {k : boto3.client(k, **credentials_applied) for k in AWS_REQUIRED_RESOURCES}
	return AWS_CLIENT, AWS_RESOURCE

AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

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

from itertools import islice

def chunked(iterable, size=1000):
	it = iter(iterable)
	while True:
		chunk = list(islice(it, size))
		if not chunk:
			break
		yield chunk

def delete_s3_keys(bucket, keys):
	results = []
	for chunk in chunked(keys, 1000):
		resp = AWS_CLIENT["s3"].delete_objects(
			Bucket=bucket,
			Delete={"Objects": [{"Key": k} for k in chunk]}
		)
		results.append(resp)
	return results

def s3_object_exists(this_bucket, this_path):
	try:
		AWS_CLIENT['s3'].head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False

def s3_cache_read(bucket, key, template_cache=dict()):
	try:
		return json.loads(AWS_RESOURCE['s3'].Object(bucket, key).get()['Body'].read())
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
			raise Exception()
		elif error_code == 'NoSuchKey':
			print(f"Could not find cache '{bucket}/{key}'")
			return dict(template_cache)
		else:
			print(f"❌ Other S3 error: {e}")
	except:
		return dict(template_cache)

def s3_cache_write(bucket, key, content):
	try:
		AWS_RESOURCE['s3'].Object(bucket, key).put(Body=json.dumps(content, indent=3))
	except ClientError as e:
		error_code = e.response['Error']['Code']
		if error_code in ['SlowDown', 'RequestTimeout', 'Throttling', 'ThrottlingException']:
			print(f"⚠️ S3 is slowing down or throttling requests: {error_code}")
		else:
			print(f"❌ Other S3 error: {e}")
		raise Exception()
	except:
		raise Exception()
		print(traceback.format_exc())
		pass

lambda_client = boto3.client("lambda", config=botocore.config.Config(
	retries={'max_attempts': 0}, 
	read_timeout=840, 
	connect_timeout=600, 
	region_name="ap-southeast-2"))

##############################################################################################################################
##############################################################################################################################
### MAIN ROUTINE
##############################################################################################################################
##############################################################################################################################

'''
	wakes up

	checks the ccl cache for scrapes that have gone through correctly and that now require download

	matches them against the download cache -> note we dont edit the ccl cache




		remove the mass download keywoird from teh ccl cache





	goes to ccl scrape files -> determines what to download

	downloads it all to the bucket


'''

def determine_download_outcome(mass_download_result):
	PERMISSIBLE_DETAILS = ["UNKNOWN_FILE_TYPE"]
	NON_PERMISSIBLE_DETAILS = ["URL_SIGNATURE_EXPIRY"]
	if ("outlinks" in mass_download_result):
		distribution = {"WELL_FORMED" : int()}
		for k in mass_download_result["outlinks"]:
			if (mass_download_result["outlinks"][k]["passed"]):
				distribution["WELL_FORMED"] += 1
			else:
				if ("detail" in mass_download_result["outlinks"][k]):
					this_detail = mass_download_result["outlinks"][k]["detail"]
					if (not this_detail in distribution):
						distribution[this_detail] = int()
					distribution[this_detail] += 1
		return {
				"status" : ("FAILURE" if (any([any([(x in y) for y in distribution]) for x in NON_PERMISSIBLE_DETAILS])) else "SUCCESS"),
				"distribution" : distribution
			}

	else:
		return {"status" : "ERROR"}

def mass_download_complete(this_ccl_uuid, ccl_data_donation_cache):
	# Find the data donations for this_ccl_uuid
	derivative_unsplit_data_donation_uuids = [k for k in ccl_data_donation_cache 
												if (this_ccl_uuid in ccl_data_donation_cache[k]["group_term_uuids"])]
	this_observer_uuid = None
	relevant_data_donation_uuids = list()
	for k in derivative_unsplit_data_donation_uuids:
		this_observer_uuid, this_data_donation_uuid = k.split("/")
		relevant_data_donation_uuids.append(this_data_donation_uuid.replace(".json", str()))

	# Reflect the ccl completion in the entrypoint cache
	entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
	entrypoint_cache = s3_cache_read(S3_OBSERVATIONS_BUCKET,entrypoint_cache_path)
	for x in relevant_data_donation_uuids:
		if (not "ccl_cache_uuids" in entrypoint_cache[x]):
			entrypoint_cache[x]["ccl_cache_uuids"] = list()
		entrypoint_cache[x]["ccl_cache_uuids"].append(this_ccl_uuid)
		entrypoint_cache[x]["ccl_cache_uuids"] = list(set(entrypoint_cache[x]["ccl_cache_uuids"]))
	s3_cache_write(S3_OBSERVATIONS_BUCKET, entrypoint_cache_path, entrypoint_cache)

	# Find the attached formalized_v2_uuids
	relevant_formalized_uuids = list(); 
	[relevant_formalized_uuids.extend(list() if (not "formalized_v2_uuids" in entrypoint_cache[x]) 
					else entrypoint_cache[x]["formalized_v2_uuids"]) for x in relevant_data_donation_uuids]

	# If there are no formalized_v2_uuids to record, it must mean that they haven't yet compiled
	if (len(relevant_formalized_uuids) == 0):
		pass
	else:
		# Otherwise commit the mass download completion to the relevant formalized cache entries
		formalized_cache = s3_cache_read(S3_OBSERVATIONS_BUCKET, f"{this_observer_uuid}/formalized_cache.json")
		for this_formalized_uuid in relevant_formalized_uuids:
			formalized_cache[this_formalized_uuid]["ccl_advertiser_scrape_v2_mass_download"] = int(time.time())
		s3_cache_write(S3_OBSERVATIONS_BUCKET, f"{this_observer_uuid}/formalized_cache.json", formalized_cache)

def routine_index_what_to_download(event, context):
	'''
		Load in the ccl cache
		
		identify all content with outcomes that have been marked as scraped - this forms our initial list of content to cross-check
	
		then load up (or instantiate) the ccl-download-cache - it needs to 
	'''
	
	# Load in the ccl cache
	ccl_cache = None
	if ("running_locally" in event):
		ccl_cache = json.loads(open(os.path.join(os.getcwd(), "ccl_cache_dummy.json")).read())
	else:
		#ccl_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_cache.json")
		ccl_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				}
			})
	
	# Isolate the entries that have outcomes marked as SCRAPED
	isolated_ccl_cache = dict()
	for k in ccl_cache:
		if (("outcome" in ccl_cache[k]) and ("status" in ccl_cache[k]["outcome"]) and (ccl_cache[k]["outcome"]["status"] in ["SCRAPED", "SCRAPED_AND_FORMALIZER_ISSUE"])):
			isolated_ccl_cache[k] = ccl_cache[k]
	
	# Load in the ccl_download_cache (or instantiate it if necessary)
	#ccl_download_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_download_cache.json")
	ccl_download_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_download_cache_distributed"
				}
			})


	# For each entry in the isolated_ccl_cache, check it against the ccl_download_cache
	# If an entry is found in the ccl_download_cache - and said entry has it such that it was executed to download AFTER the entry in the ccl_cache, then we
	# can assert that the download went through without issue
	# Otherwise, if any of the former conditions are not met, we must record the entry in the list of entries requiring download
	to_download = list()
	for k in isolated_ccl_cache:
		if (k in ccl_download_cache) and (ccl_download_cache[k]["downloaded_at"] > isolated_ccl_cache[k]["outcome"]["at"]):
			# The download has already been carried out
			pass
		else:
			to_download.append(ccl_cache[k])

	# Sort the to_download list (newest to oldest to tackle URL signature issues)
	to_download = sorted(to_download, key=lambda x: x["outcome"]["at"], reverse=True)

	if ("shuffle" in event):
		random.shuffle(to_download)

	'''
		for each entry to download (the files for)
		we need to firstly enumerate the files to download and create a tentative download_check_cache
		at the end of each file download attempt, the cache is checked - if the download_check_cache is complete, the ccl_download_cache is finally updated
		the list that is handed to the next part of the instance is aggregated across entries
	'''
	MAX_N_TO_DOWNLOAD = 500
	# Go through each entry of the to_download list, and retrieve its files that are intended for download
	n_entries_for_dispatcher = int()
	to_dispatch_to_downloader = list()
	'''
	to_download = [
		{
			"uuid" : "205212d1-5e25-494a-8824-c3d8f8e8e73d", # downloadable
			"platform" : "FACEBOOK"
		}
	]
	'''

	'''
	to_download = [
		{
			"uuid" : "00d2d14c-289e-4c05-ac6d-3e77383052a2", # URL expiry
			"platform" : "FACEBOOK"
		}
	]
	'''
	'''
	to_download = [
		{
			"uuid" : "013d3057-9495-43c2-bd29-928ddc8acbcb", # No outlinks
			"platform" : "FACEBOOK"
		}
	]
	'''

	to_reflect_as_prematurely_downloaded = list()
	to_remediate = list()
	#ccl_data_donation_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_data_donation_cache.json")
	ccl_data_donation_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				},
				"read_keys" : [x["observer_uuid"] for x in to_download] # Note that the 'to_download' derives from entries of the ccl_cache
			})
	for x in to_download:
		this_vendor = CCL_CONFIG["platform_vendor_mappings"][x["platform"]]
		if (this_vendor == "meta_adlibrary"):
			# Scrape bucket is located
			scrape_bucket_key = f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{x["uuid"]}'
			scrape_output_key = f'{scrape_bucket_key}/scrape_output.json'
			scrape_output = json.loads(AWS_RESOURCE['s3'].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, scrape_output_key).get()['Body'].read())
			try:
				# If the outlinks are empty, then we bypass the download phase, and update the download cache to reflect this
				if (len(scrape_output["response_interpreted"]["outlinks"]) == 0):
					to_reflect_as_prematurely_downloaded.append({"uuid" : x["uuid"], "outcome" : {"status" : "NO_OUTLINKS"}})
					mass_download_complete(x["uuid"], ccl_data_donation_cache)
				else:
					# If there already exists a mass_download_result, check it to determine if the download has already been executed...
					mass_download_result_key = f'{scrape_bucket_key}/mass_download/mass_download_result.json'
					if (s3_object_exists(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, mass_download_result_key)):
						# if we stumble upon a partially done download (which can be done by examining the mass download result - if it exists ofc)
						# we should then attempt to finish the download instead of starting again
						#
						# Examine the mass download result - if it is complete, go no further with it...
						partial_mass_download_result = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, mass_download_result_key)
						complete = True
						for k in partial_mass_download_result["outlinks"]:
							if (not partial_mass_download_result["outlinks"][k]["attempted"]):
								to_dispatch_to_downloader.append(partial_mass_download_result["outlinks"][k])
								complete = False
						if (complete):
							# In this case, we need to indicate to the ccl_download_cache that the 
							to_reflect_as_prematurely_downloaded.append({"uuid" : x["uuid"], "outcome" : determine_download_outcome(partial_mass_download_result)})
					else:
						outlinks_to_download = list()
						for this_outlink in scrape_output["response_interpreted"]["outlinks"]:
							this_outlink_uuid = str(uuid.uuid4())
							downloadable_obj = {
									"vendor" : this_vendor,
									"url" : this_outlink,
									"scrape_uuid" : x["uuid"],
									"outlink_uuid" : this_outlink_uuid
								}
							to_dispatch_to_downloader.append(downloadable_obj)
							outlinks_to_download.append(downloadable_obj)
						n_entries_for_dispatcher += 1
						# The mass download result is generated, and the outlinks to download are enumerated
						s3_cache_write(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, mass_download_result_key, {
								"outlinks" : {x["outlink_uuid"]:x | { "attempted" : False } for x in outlinks_to_download}
							})
			except:
				to_remediate.append(x | {"trace" : str(traceback.format_exc())})
				pass
		if (len(to_dispatch_to_downloader) > MAX_N_TO_DOWNLOAD):
			to_dispatch_to_downloader = to_dispatch_to_downloader[:MAX_N_TO_DOWNLOAD]
			print("Breaking early to avoid overload...")
			break
	print(f"A total of {len(to_remediate)} entries need remediation")
	print(f"A total of {n_entries_for_dispatcher} entries containing {len(to_dispatch_to_downloader)} files were indexed for download")

	# Load in the download cache (again) as it may've been modified since our last access
	#ccl_download_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_download_cache.json")
	# For each entry in the to_download list that should be reflected as prematurely downloaded, finalise them prematurely
	ccl_download_cache_appendage = dict()
	for x in to_download:
		premature_reflection_candidates = [y for y in to_reflect_as_prematurely_downloaded if (y["uuid"] == x["uuid"])]
		if (len(premature_reflection_candidates) > 0):
			ccl_download_cache_appendage[x["uuid"]] = { 
					"uuid" : x["uuid"],
					"downloaded_at" : int(time.time()), 
					"outcome" : premature_reflection_candidates[0]["outcome"]
				} 
	# Write the result of the premature downloads...
	#s3_cache_write(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_download_cache.json", ccl_download_cache)
	distributed_cache_write({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_download_cache_distributed"
			},
			"longitudinal_unit" : A_DAY,
			"longitudinal_key" : ["downloaded_at"],
			"input" : ccl_download_cache_appendage
		})

	# Finally, send off the items that need to be downloaded
	invoke_response = lambda_client.invoke(
		FunctionName=context.invoked_function_arn, InvocationType='Event', 
		Payload=json.dumps({ 
			"action": "download",
			"content_to_download" : to_dispatch_to_downloader
		}))

	return str()



'''
	This is just a helper function for redistributing mass download proxies on the Meta Ad Library scrape identities
'''
def redistribute_scrape_identities():
	'''
		Load in each scrape identity
		adjust to reflect redistribution
		put back
	'''
	scrape_identities = get_list_objects_v2(Bucket="fta-mobile-observations-holding-bucket", Prefix="scrape_identities/")
	weighted_dist = {k:int() for k in ["3.106.201.18", "16.176.169.11", "3.24.139.9"]}
	for x in scrape_identities["Contents"]:
		selected_ip_address = None
		for k in weighted_dist:
			if (weighted_dist[k] == min(weighted_dist.values())):
				weighted_dist[k] += 1
				selected_ip_address = k
				break
		print(x["Key"])
		this_scrape_identity = s3_cache_read("fta-mobile-observations-holding-bucket",x["Key"])
		for alt in ["http", "https"]:
			this_scrape_identity["proxies"]["mass_downloads"][alt] = f"http://adms2021:Algorithm1@{selected_ip_address}:8888/"
		s3_cache_write("fta-mobile-observations-holding-bucket",x["Key"], this_scrape_identity)
		#print(json.dumps(this_scrape_identity, indent=3))
	print(json.dumps(weighted_dist, indent=3))


'''
	This function retrieves an available scrape identity for a given platform
'''
def get_available_scrape_identity_downloader(vendor):
	# Load in the scrape identities
	scrape_identities = get_list_objects_v2(Bucket="fta-mobile-observations-holding-bucket", Prefix="scrape_identities/")
	scrape_identities_instantiated = list()
	for x in scrape_identities["Contents"]:
		scrape_identities_instantiated.append(json.loads(AWS_RESOURCE["s3"].Object("fta-mobile-observations-holding-bucket",x["Key"]).get()['Body'].read()))

	available_scrape_identities = [x for x in scrape_identities_instantiated if ((x["vendor"] == vendor.upper()) and (x["valid"]))]
	if (len(available_scrape_identities) > 0):
		# Retrieve one at random
		random.shuffle(available_scrape_identities)
		return available_scrape_identities[0]
	else:
		if (__name__ == "__main__"):
			ipdb.set_trace()
		else:
			raise Exception("No available scrape identity...")




def routine_download_v2(event, context):
	# Retrieve a scrape identity
	call_time = int(time.time())
	proxies_mass_downloads = dict()
	distinct_vendors = list(set([x["vendor"] for x in event["content_to_download"]]))
	for this_vendor in distinct_vendors:
		this_scrape_identity = get_available_scrape_identity_downloader(this_vendor)
		proxies_mass_downloads[this_vendor] = this_scrape_identity["proxies"]["mass_downloads"]

	# For the contents, head requests are undertaken - while it may seem sensible to only specify for those that return 
	# images or videos, we do also want to yield instances of scrape failure and so anticipate this too...
	# Note: 'Outlink scraping' is common to multiple functions, hence why it isn't written multiple times
	SCRAPEABLE_CONTENT_TYPES = {
			"image" : "jpeg", 
			"video" : "mp4", 
			"application/octet-stream" : "mp4",
			"text/plain" : "txt"
		}

	content_to_download = list(event["content_to_download"])


	# Prior to performing the downloads, the mass_download_result is retrieved for each distinct scrape_bucket_key (if it exists)
	#
	# This is to prevent constant reading/writing...
	#
	# Otherwise its instantiated at the end... 
	distinct_scrape_uuids = [x["scrape_uuid"] for x in content_to_download]
	mass_download_result_dict = dict()
	for k in distinct_scrape_uuids:
		scrape_bucket_key = f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{k}'
		mass_download_result_key = f'{scrape_bucket_key}/mass_download/mass_download_result.json'
		# We technically don't need to check if the content exists, but safe to do so anyway
		if (s3_object_exists(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, mass_download_result_key)):
			mass_download_result_dict[k] = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,mass_download_result_key)
		else:
			mass_download_result_dict[k] = {"outlinks" : dict()}

	while ((len(content_to_download) > 0) and (abs(time.time() - call_time) < MAX_EXECUTION_TIME)):
		# Pick the first element off the list
		this_element = dict(content_to_download[0])
		# Retrieve the head of the file...
		try:
			this_element["content_type"] = requests.head(this_element["url"], 
					timeout=3, proxies=proxies_mass_downloads[this_element["vendor"]]).headers["Content-Type"]
		except:
			# We pick up absences in later steps
			this_element["content_type"] = "UNKNOWN"

		# If the file is scrapeable (dictated by file extension)
		nominated_content_type = [x for x in SCRAPEABLE_CONTENT_TYPES if (x in this_element["content_type"])]

		try:
			this_element["attempted"] = True
			if (len(nominated_content_type) > 0):
				this_element["passed"] = False
				# Retrieve the file extension
				applied_file_extension = SCRAPEABLE_CONTENT_TYPES[nominated_content_type[0]]
				# Assemble the filename
				fname = f"{this_element['outlink_uuid']}.{applied_file_extension}"
				# Attempt to run the request...
				output_key = f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_element["scrape_uuid"]}/mass_download/{fname}'
				response = requests.get(this_element["url"], stream=True, proxies=proxies_mass_downloads)
				if (response.status_code == 200):
					if ((applied_file_extension == "txt") and ("URL signature expired" in response.raw.read().decode('utf-8'))):
						this_element["detail"] = "URL_SIGNATURE_EXPIRY"
					else:
						AWS_RESOURCE["s3"].Bucket(S3_MOBILE_OBSERVATIONS_CCL_BUCKET).upload_fileobj(response.raw, output_key)
						this_element["passed"] = True
						this_element["content_type"] = applied_file_extension
				elif (response.status_code == 403):
					this_element["detail"] = "URL_SIGNATURE_EXPIRY"
				else:
					this_element["detail"] = f"STATUS_CODE:{str(response.status_code)}"
			else:
				this_element["passed"] = False
				this_element["detail"] = "UNKNOWN_FILE_TYPE"
		except:
			this_element["detail"] = "ERROR"
			this_element["trace"] = str(traceback.format_exc())
		mass_download_result_dict[this_element["scrape_uuid"]]["outlinks"][this_element["outlink_uuid"]] = this_element
		# Remove the element off the list
		del content_to_download[0]


	# After the process, commit the mass_download_result_dict, and the ccl_download_cache if possible
	for scrape_uuid in mass_download_result_dict:
		# Update the mass_download_result_dict
		scrape_bucket_key = f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{scrape_uuid}'
		mass_download_result_key = f'{scrape_bucket_key}/mass_download/mass_download_result.json'
		s3_cache_write(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, mass_download_result_key, mass_download_result_dict[scrape_uuid])
		if (all([mass_download_result_dict[scrape_uuid]["outlinks"][outlink_uuid]["attempted"] 
						for outlink_uuid in mass_download_result_dict[scrape_uuid]["outlinks"]])):
			# Can update the ccl_download_cache
			#
			# Load in the download cache (again) as it may've been modified since our last access
			#ccl_download_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_download_cache.json")
			ccl_download_cache_appendage = dict()
			ccl_download_cache_appendage[scrape_uuid] = { 
				"scrape_uuid" : scrape_uuid,
				"downloaded_at" : int(time.time()),
				"outcome" : determine_download_outcome(mass_download_result_dict[scrape_uuid])
			}
			#s3_cache_write(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_download_cache.json", ccl_download_cache)
			distributed_cache_write({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_download_cache_distributed"
				},
				"longitudinal_unit" : A_DAY,
				"longitudinal_key" : ["downloaded_at"],
				"input" : ccl_download_cache_appendage
			})

			if (ccl_download_cache_appendage[scrape_uuid]["outcome"]["status"] in PERMISSIBLE_OUTCOMES_FOR_COMPLETION):
				#ccl_data_donation_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_data_donation_cache.json")
				ccl_data_donation_cache = distributed_cache_read({
							"cache" : {
								"bucket" : "fta-mobile-observations-v2-ccl",
								"path" : "ccl_data_donation_cache_distributed"
							}
						})
				mass_download_complete(scrape_uuid, ccl_data_donation_cache)


	# If the process not is complete
	if (len(content_to_download) > 0):
		# Call to itself with remaining outlinks for download
		invoke_response = lambda_client.invoke(
			FunctionName=context.invoked_function_arn, InvocationType='Event', 
			Payload=json.dumps({ 
				"action": "download",
				"content_to_download" : content_to_download
			}))
	return str()


# TODO - the repair function of the moat downloader (which supervises and corrects url sig expiries) is not automatic
# nor do we have reporting it <<<< it should technically be a manual process to remediate

	

'''
	TODO

		from the general statistics, we can observe a failure set

		the failure set comprises 'failures due to url expiry' and 'failures due to other reasons'

		for 'failure due to url expiry'

			use the outputs from this set to examine those routines that have large intervals between scrapes and downloads and those that dont

				ie.

					large intervals vs small intervals

			large intervals are assumed to be straightforward (the urls expired)

			small intervals are indicative of perhaps a scraper identity compromisation. hence find out if there are consistent scrape 
			identities that are affected by the compromisation

				assess distribution of scrape identities assoicated with 'failures due to url expiry' that have 'small interval' and compare them
				to recent distribution of scrape identities associated successful scrapes

				if there is a distinction between both sets, replacement of scrape identity is warranted - otherwise we can only really go back and rescrape

		once this routine is correctly defined to do scraper identity replacement (if warranted), we can invent the rollback routine to go back and do rescrapes


		UPDATE: It was found that while there were no patterns of contrast between dates of collection, there was however a 3-day period during which a
		sizeable number of moat_downloads failed - this is going to be treated as an isolated incident (perhaps triggered on Meta's end) - the rollback will be undertaken
		and we will observe the newer outcomes as they come to light.
'''


def subroutine_get_failure_set(ccl_cache, local_statistics):
	failure_set = {
			"due_to_url_expiry" : list(),
			"other" : list()
		}
	for k in local_statistics["failures"]:
		# We go over entries that have url expiries, selecting only those that have significant expiries
		# (i.e. greater URL expiries than WELL_FORMED entries where the total number of requested image or video resources is of a majority URL expiry)
		if (("URL_SIGNATURE_EXPIRY" in local_statistics["failures"][k]["outcome"]["distribution"]) 
			and (local_statistics["failures"][k]["outcome"]["distribution"]["URL_SIGNATURE_EXPIRY"] > 0)
			and (local_statistics["failures"][k]["outcome"]["distribution"]["WELL_FORMED"] < local_statistics["failures"][k]["outcome"]["distribution"]["URL_SIGNATURE_EXPIRY"])
			and ((local_statistics["failures"][k]["outcome"]["distribution"]["URL_SIGNATURE_EXPIRY"] / sum(list(local_statistics["failures"][k]["outcome"]["distribution"].values()))) > 0.5)):
			failure_set["due_to_url_expiry"].append(k)
		else:
			failure_set["other"].append(k)
	return failure_set

'''
	At the end of 'moat_download' routines, we get a clear indication of what succeeded and what failed

	This local routine diagnoses failures, providing statistics on scraper identity distribution for failures, as well as whether the failures happened in a certain time range

	Note: While the routine was able to determine the spread of failures, no clear pattern was seen, falsifying the assumption that scraper identities fail after
	some time - as a new assumption, it is considered that the small time range of the failures isolate it.

	Stages:
		* LOCAL_STATISTICS
		* GET_OUTPUTS
		* DIAGNOSIS
'''
def routine_local_management_meta_adlibrary(case="GET_OUTPUTS", subcase=None):
	if (case == "LOCAL_STATISTICS"):
		#ccl_download_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_download_cache.json")
		ccl_download_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" :  "ccl_download_cache_distributed"
				}
			})
		with open(os.path.join(os.getcwd(), "ccl_download_cache.json"), "w") as f:
			f.write(json.dumps(ccl_download_cache, indent=3))
			f.close()
		#ccl_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_cache.json")
		ccl_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				}
			})
		with open(os.path.join(os.getcwd(), "ccl_cache.json"), "w") as f:
			f.write(json.dumps(ccl_cache, indent=3))
			f.close()
		weighted = dict()
		statistics = {
			"weighted" : dict(),
			"failures" : dict(),
			"successes_uuids" : list()
		}
		for x in ccl_download_cache:
			this_status = ccl_download_cache[x]["outcome"]["status"]
			if (not this_status in statistics["weighted"]):
				statistics["weighted"][this_status] = int()
			statistics["weighted"][this_status] += 1
			if (this_status == "FAILURE"):
				statistics["failures"][x] = ccl_download_cache[x]
			elif (this_status == "SUCCESS"):
				statistics["successes_uuids"].append(x)
		with open(os.path.join(os.getcwd(), "local_statistics.json"), "w") as f:
			f.write(json.dumps(statistics, indent=3))
			f.close()
	else:
		# Firstly determine what succeeded and what failed
		if ((case == "ROLLBACKS") and (subcase == "CACHE_CHANGES")):
			s3_key_movements_path = os.path.join(os.getcwd(), "s3_key_movements.json")
			s3_key_movements = json.loads(open(s3_key_movements_path).read())
			#
			# Thirdly, we adjust the necessary caches (ccl_cache, ccl_download_cache)
			# Note: We need not make any adjustments beyond this, as the ccl_advertiser_scrape_v2_mass_download keyword will be supplied to the formalized_cache for the relevant observer
			# at the end of the newer scrape
			#
			# NOTE: This should be executed on the cloud
			#
			ccl_uuids = list(set([x["from"].split("/")[3] for x in s3_key_movements]))
			#
			ccl_cache = distributed_cache_read({
					"cache" : {
						"bucket" : "fta-mobile-observations-v2-ccl",
						"path" : "ccl_cache_distributed"
					}
				})
			for x in ccl_uuids:
				if (x in ccl_cache):
					ccl_cache[x] = {k:v for k,v in ccl_cache[x].items() if (not k == "outcome")}
			distributed_cache_write({
					"cache" : {
						"bucket" : S3_MOBILE_OBSERVATIONS_CCL_BUCKET,
						"path" : "ccl_cache_distributed"
					},
					"longitudinal_unit" : A_DAY,
					"longitudinal_key" : ["timestamp"],
					"input" : ccl_cache
				})
			#
			#ccl_download_cache = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,"ccl_download_cache.json")
			ccl_download_cache = distributed_cache_read({
					"cache" : {
						"bucket" : "fta-mobile-observations-v2-ccl",
						"path" :  "ccl_download_cache_distributed"
					}
				})
			'''
				for x in ccl_uuids:
					if (x in ccl_download_cache):
						del ccl_download_cache[x]
			'''
			entries_to_delete = dict()
			for x in ccl_uuids:
				if (x in ccl_download_cache):
					entries_to_delete[x] = ccl_download_cache[x]
			#s3_cache_write(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_download_cache.json", ccl_download_cache)
			distributed_cache_write({
					"cache" : {
						"bucket" : "fta-mobile-observations-v2-ccl",
						"path" : "ccl_download_cache_distributed"
					},
					"longitudinal_unit" : A_DAY,
					"longitudinal_key" : ["downloaded_at"],
					"delete" : True,
					"input" : entries_to_delete
				})
			#ipdb.set_trace()
		else:
			ccl_cache = json.loads(open(os.path.join(os.getcwd(), "ccl_cache.json")).read())
			local_statistics = json.loads(open(os.path.join(os.getcwd(), "local_statistics.json")).read())
			failure_set = subroutine_get_failure_set(ccl_cache, local_statistics)
			# Print the results
			print("Breakdown of scrape failure types:")
			for k in failure_set:
				print("\t", k, len(failure_set[k]))
			
			# Export the ccl_uuids as their scrape output paths
			if (case == "GET_OUTPUTS"):
				scrape_outputs_path = os.path.join(os.getcwd(), "scrape_outputs")
				os.mkdir(scrape_outputs_path)
				with open("to_download.txt", "w") as f:
					for k in due_to_url_expiry_breakdown:
						for x in due_to_url_expiry_breakdown[k]:
							source_path = f's3://{S3_MOBILE_OBSERVATIONS_CCL_BUCKET}/outputs/meta_adlibrary/meta_adlibrary_scrapes/{x["ccl_uuid"]}/scrape_output.json'
							dest_path = os.path.join(scrape_outputs_path, x["ccl_uuid"])
							f.write(f'{source_path} {dest_path}\n')
					f.close()
				print('''
					Note: At the end of GET_OUTPUTS case, run the following (assuming MacOS with parallel installed and AWS profile):

					parallel -j 5 --colsep ' ' '
						mkdir -p "$(dirname {2})";
						aws s3 cp {1} {2}
						' :::: to_download.txt
					''')
			elif (case == "DIAGNOSIS"):
				A_DAY = 24 * 60 * 60
				due_to_url_expiry_breakdown = {k:list() for k in ["large_interval", "small_interval"]}
				for k in failure_set["due_to_url_expiry"]:
					time_of_scrape = ccl_cache[k]["outcome"]["at"]
					time_of_download = local_statistics["failures"][k]["downloaded_at"]
					scrape_output = json.loads(open(os.path.join(os.getcwd(), "scrape_outputs", k)).read())#s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET,f"outputs/meta_adlibrary/meta_adlibrary_scrapes/{k}/scrape_output.json")
					scrape_identity_uuid = "UNKNOWN" if (not "scrape_identity_uuid" in scrape_output) else scrape_output["scrape_identity_uuid"]
					tangible_result = {"ccl_uuid" : k, "scraper_identity_uuid" : scrape_identity_uuid, "time_of_scrape" : time_of_scrape}
					if (abs(time_of_scrape - time_of_download) > A_DAY):
						due_to_url_expiry_breakdown["large_interval"].append(tangible_result)
					else:
						due_to_url_expiry_breakdown["small_interval"].append(tangible_result)
			
				print("Breakdown of interval types in failures due_to_url_expiry:")
				for k in due_to_url_expiry_breakdown:
					due_to_url_expiry_breakdown[k] = sorted(due_to_url_expiry_breakdown[k], key=lambda x : x["time_of_scrape"])
					print("\t", k, len(due_to_url_expiry_breakdown[k]))

				for k in due_to_url_expiry_breakdown:
					print(f"Scraper identity breakdown of failures ({k}):")
					print(json.dumps(dict(Counter([x["scraper_identity_uuid"] for x in due_to_url_expiry_breakdown[k]])), indent=3))

				import datetime as dt
				import matplotlib.pyplot as plt
				import matplotlib.dates as mdates

				def plot_daily_frequency_min_7days(unix_timestamps):
					"""
					Plot a bar chart of counts per day, forcing a minimum 7-day window.

					Parameters
					----------
					unix_timestamps : list[int|float]

					Returns
					-------
					(fig, ax)
					"""
					if not unix_timestamps:
						raise ValueError("unix_timestamps list is empty")

					# Convert timestamps → local dates
					dates = [dt.datetime.fromtimestamp(ts).date() for ts in unix_timestamps]

					# Determine natural min/max dates
					start_date = min(dates)
					end_date = max(dates)

					# Enforce a minimum 7-day window (inclusive range)
					natural_days = (end_date - start_date).days + 1
					min_days_required = 7

					if natural_days < min_days_required:
						# Extend the end date so the window is exactly 7 days
						end_date = start_date + dt.timedelta(days=min_days_required - 1)

					# Build full date range for plotting
					total_days = (end_date - start_date).days + 1
					all_days = [start_date + dt.timedelta(days=i) for i in range(total_days)]

					# Count occurrences on each date
					counts = Counter(dates)
					frequencies = [counts.get(day, 0) for day in all_days]

					# Plot
					fig, ax = plt.subplots()
					ax.bar(all_days, frequencies, width=0.8)

					ax.set_xlabel("Date")
					ax.set_ylabel("Frequency")
					ax.set_title("Daily Frequency of Timestamps (Min. 7-Day Window)")

					ax.xaxis.set_major_locator(mdates.AutoDateLocator())
					ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
					fig.autofmt_xdate()

					plt.tight_layout()
					plt.show()

				plot_daily_frequency_min_7days([x["time_of_scrape"] for x in due_to_url_expiry_breakdown["small_interval"]])
				ipdb.set_trace()
			elif (case == "ROLLBACKS"):
				# We shift all existing resources in the scrape to a rollback entry
				s3_key_movements_path = os.path.join(os.getcwd(), "s3_key_movements.json")
				s3_key_movements = list()
				if (not os.path.exists(s3_key_movements_path)):
					i = int()
					for x in failure_set["due_to_url_expiry"]:
						print(x)
						i += 1
						print(f'{i}/{len(failure_set["due_to_url_expiry"])}')
						this_prefix = f"outputs/meta_adlibrary/meta_adlibrary_scrapes/{x}"
						#results = subbucket_contents({"Bucket" : S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "Prefix" : this_prefix})
						results_b = get_list_objects_v2(Bucket=S3_MOBILE_OBSERVATIONS_CCL_BUCKET, Prefix=this_prefix)
						results_c = list()
						if ("Contents" in results_b): results_c = [y["Key"] for y in results_b["Contents"]]
						rollback_signature = int(time.time())
						for y in results_c:
							s3_key_movements.append({
									"from" : y,
									"to" : f"{this_prefix}/rollbacks/{rollback_signature}/{y.replace(this_prefix+'/', str())}"
								})
					with open(s3_key_movements_path, "w") as f: f.write(json.dumps(s3_key_movements,indent=3))
				else:
					s3_key_movements = json.loads(open(s3_key_movements_path).read())
				if (subcase == "COPY"):
					# Undertake the mass-copy event
					for this_entry in s3_key_movements:
						print(this_entry)
						AWS_CLIENT['s3'].copy_object(
							Bucket=S3_MOBILE_OBSERVATIONS_CCL_BUCKET,
							CopySource={"Bucket": S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "Key": this_entry["from"]},
							Key=this_entry["to"]
						)
				elif (subcase == "DELETE"):
					# Undertake the deletions
					# TODO - WARNING - this has not performed correctly in the past and caused unnecessary data deletion - investigate and correct before proceeding with its usage in future
					delete_s3_keys(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, [x["from"] for x in s3_key_movements])



if (__name__ == "__main__"):
	routine_local_management_meta_adlibrary(case="LOCAL_STATISTICS")
	pass
	#routine_local_management_meta_adlibrary(case="ROLLBACKS",subcase="CACHE_CHANGES")

'''
if (__name__ == "__main__"):
	#routine_index_what_to_download({"running_locally" : True}, None)
	routine_download_v2({
			"content_to_download" : [
				{
					"vendor" : "meta_adlibrary",
					"scrape_uuid" : "00d2d14c-289e-4c05-ac6d-3e77383052a2",
					"outlink_uuid" : "fa0e8f9c-43df-48e2-9946-36dabc9b71a8",
					"url" : "https://scontent-syd2-1.xx.fbcdn.net/v/t42.1790-2/509914246_1249917939819978_8507166369753050807_n.?_nc_cat=109&ccb=1-7&_nc_sid=c53f8f&_nc_ohc=UGFndO--iA8Q7kNvwHotvYD&_nc_oc=Adk3b5Gi45w8pLY2v7Dj4YCdKe5xAbXjO4wH3lLfq6bpUBpK9mwSq3BE7-0JNwkUf8o&_nc_zt=28&_nc_ht=scontent-syd2-1.xx&_nc_gid=EdFP0B4q0Cw57KVoL8LNKA&oh=00_AfZSCTY0qiFEdV9n31CJjRJr_6tAcCGPB6GXios2QGjbBA&oe=68E25CF7"
				}
			]
		}, None)

ipdb.set_trace()
'''
'''
def routine_download(event=None, running_locally=False , context=None):
	# Retrieve a scrape identity
	call_time = int(time.time())
	this_scrape_identity = get_available_scrape_identity_downloader(event["vendor"])
	proxies_mass_downloads = this_scrape_identity["proxies"]["mass_downloads"]

	# For the contents, head requests are undertaken - only those that return images or videos are actually scraped
	# A data structure is provided with the content that allows us to correctly index it
	#
	# Note: 'Outlink scraping' is common to multiple functions, hence why it isn't written multiple times
	SCRAPEABLE_CONTENT_TYPES = {
			"image" : "jpeg", 
			"video" : "mp4", 
			"application/octet-stream" : "mp4"
		}

	download_log = dict()
	content_to_download = list(event["content_to_download"])
	while ((len(content_to_download) > 0) and (abs(time.time() - call_time) < MAX_EXECUTION_TIME)):
		# Pick the first element off the list
		this_element = dict(content_to_download[0])
		# Retrieve the head of the file...
		try:
			this_element["content_type"] = requests.head(this_element["url"], 
						timeout=3, proxies=proxies_mass_downloads).headers["Content-Type"]
		except:
			# We pick up absences in later steps
			this_element["content_type"] = "UNKNOWN"
		# If the file is scrapeable (dictated by file extension)
		nominated_content_type = [x for x in SCRAPEABLE_CONTENT_TYPES if (x in this_element["content_type"])]
		if (len(nominated_content_type) > 0):
			this_element["scrape_attempted"] = True
			this_element["scrape_passed"] = False
			# Retrieve the file extension
			applied_file_extension = SCRAPEABLE_CONTENT_TYPES[nominated_content_type[0]]
			# Assemble the filename
			fname = f"{str(uuid.uuid4())}.{applied_file_extension}"
			# Attempt to run the request...
			response = requests.get(this_element["url"], stream=True, proxies=proxies_mass_downloads)
			if (response.status_code == 200):
				AWS_RESOURCE["s3"].Bucket(S3_OBSERVATIONS_BUCKET).upload_fileobj(
					response.raw, f'{event["output_location"]}/{fname}')
				this_element["scrape_passed"] = True
				this_element["key"] = fname
				this_element["content_type"] = applied_file_extension
		else:
			fname = f"{str(uuid.uuid4())}.unknown"
			this_element["scrape_attempted"] = False
			this_element["scrape_passed"] = False
		download_log[fname] = this_element
		# Remove the element off the list
		del content_to_download[0]

	# Load in the metadata_obj if it already exists
	metadata_obj = { "download_log" : dict() }
	metadata_path = f'{event["output_location"]}/metadata.json'
	if (s3_object_exists(S3_OBSERVATIONS_BUCKET, metadata_path)):
		metadata_obj = json.loads(AWS_RESOURCE["s3"].Object(S3_OBSERVATIONS_BUCKET, metadata_path).get()['Body'].read())
	metadata_obj["download_log"].update(download_log)
	# Update after instantiation, as this is relative to th current process
	# Note: In the past, we would've inserted this in, however its obsolete, considering that you can
	# assume there might be two simultaneous processes working on the same download job
	#metadata_obj["content_to_download"] = content_to_download
	AWS_RESOURCE["s3"].Object(S3_OBSERVATIONS_BUCKET, metadata_path).put(Body=json.dumps(metadata_obj, indent=3))

	# If the process not is complete
	if (len(content_to_download) > 0):
		# Call to itself with remaining outlinks for download
		lambda_client = boto3.client("lambda", config=botocore.config.Config(
			retries={'max_attempts': 0}, 
			read_timeout=840, 
			connect_timeout=600, 
			region_name="ap-southeast-2"))
		invoke_response = lambda_client.invoke(
			FunctionName=context.invoked_function_arn, InvocationType='Event', 
			Payload=json.dumps({ 
				"action": "download",
				"vendor" : event["vendor"],
				"output_location" : event["output_location"],
				"content_to_download" : content_to_download
			}))
	return str()
'''

def routine_index_what_to_download_shuffled(event, context):
	routine_index_what_to_download(event | {"shuffle" : True}, context)


def cache_changes_on_rollback(event, context):
	routine_local_management_meta_adlibrary(case="ROLLBACKS",subcase="CACHE_CHANGES")

def lambda_handler(event, context=None):
	commands = {
			"download" : routine_download_v2,
			"index" : routine_index_what_to_download,
			"index_and_shuffle" : routine_index_what_to_download_shuffled,
			#"local_repair_pt_3" : local_repair_pt_3,
			#"local_repair_pt_4" : local_repair_pt_4,
			"cache_changes_on_rollback" : cache_changes_on_rollback
		}
	return {
			'statusCode': 200,
			'body': commands[event["action"]](event, context=context)
		}
'''
def chunks_of_n_size(l, n):
	return [l[i:i+n] for i in range(int(),len(l),n)]

if (__name__ == "__main__"):

	qs_enrichment_scrapes = json.loads(open("/Users/obei/Developer/2024/_11_moat_ccl/qs_enrichment_scrapes.json").read())
	qs_as_dir = "/Users/obei/Developer/2024/_11_moat_ccl/qs_as"
	qs_as_alt_dir = "/Users/obei/Developer/2024/_11_moat_ccl/qs_as_alt"
	qs_rel_file = "/Users/obei/Developer/2024/_11_moat_ccl/qs_relational_ds_as_list.json"
	qs_rel_file_corrected = "/Users/obei/Developer/2024/_11_moat_ccl/qs_relational_ds_as_list_corrected.json"
	qs_rel = json.loads(open(qs_rel_file).read())
	qs_rel_corrected = json.loads(open(qs_rel_file_corrected).read())
	qs_as_dir_contents = os.listdir(qs_as_dir)
	qs_as_alt_dir_contents = os.listdir(qs_as_alt_dir)
	files = qs_as_dir_contents + qs_as_alt_dir_contents
	print(len(files))
	ii = int()
	for x in files:
		ii += 1
		print(ii)
		try:
			parts = x.split(".")
			if (not "DS_Store" in x):
				# For each, check if the metadata file exists - if not, go ahead and populate it
				user_uuid = parts[0]
				rdo_uuid = parts[1]
				this_index = int(parts[2])
				#
				ad_scrape_dummy = None
				this_timestamp = None
				if (x in qs_as_dir_contents):
					ad_scrape_dummy = json.loads(open(f"{qs_as_dir}/{user_uuid}.{rdo_uuid}.{this_index}.json").read())
					this_timestamp = qs_rel[this_index]["timestamp"]
				else:
					ad_scrape_dummy = json.loads(open(f"{qs_as_alt_dir}/{user_uuid}.{rdo_uuid}.{this_index}.json").read())
					this_timestamp = qs_rel_corrected[this_index]["timestamp"]

				rdo_uuid_unsplit = [x["rdo_uuid_unsplit"] for x in qs_enrichment_scrapes 
					if (x["rdo_uuid_unsplit"].startswith(str(this_timestamp)) and x["rdo_uuid_unsplit"].endswith(rdo_uuid))][0]
				this_output_location = f'{user_uuid}/ccl/{rdo_uuid_unsplit}/medias'
				if (not s3_object_exists(S3_OBSERVATIONS_BUCKET, f'{this_output_location}/metadata.json')):
					print(f"Executing {this_output_location}")
					print(len(ad_scrape_dummy["response_interpreted"]["outlinks"]))
					lambda_client = boto3.client("lambda", config=botocore.config.Config(
						retries={'max_attempts': 0}, 
						read_timeout=840, 
						connect_timeout=600, 
						region_name="ap-southeast-2"))
					MAX_N_TO_DOWNLOAD = 500
					chunks = chunks_of_n_size([{ "url" : x } for x in ad_scrape_dummy["response_interpreted"]["outlinks"]], MAX_N_TO_DOWNLOAD)
					for this_chunk in chunks:
						invoke_response = lambda_client.invoke(
							FunctionName="arn:aws:lambda:ap-southeast-2:519969025508:function:moat_downloader", 
							InvocationType=('Event' if (len(chunks) == 1) else 'RequestResponse'), 
							Payload=json.dumps({ 
								"action": "download",
								"vendor" : "meta_adlibrary",
								"output_location" : this_output_location,
								"content_to_download" : this_chunk
							}))
					if (len(ad_scrape_dummy["response_interpreted"]["outlinks"]) != 0):
						time.sleep(20)

					# TODO - do same with alt folder
				else:
					print(f"Passing {this_output_location}")
		except:
			print(traceback.format_exc())
			ipdb.set_trace()
			print(f"Error on {x}")
			pass
'''

