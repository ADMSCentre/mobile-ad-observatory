import sys
import os
import time
if (__name__ == "__main__"):
	import ipdb
import re
import boto3
import uuid
import json
import random
import traceback
import botocore
import statistics
from distributed_cache import *

OCR_DATA_CONFIDENCE_THRESHOLD = 0.6
MAX_EXECUTION_TIME = 60*13 # 13 minutes
MAX_LEVENSHTEIN_DISTANCE = 2
ACQUISITION_PLATFORM_IDENTIFIERS = {
		"facebook-light" : "facebook",
		"facebook-dark" : "facebook",
	}

EXPOSURE_IDENTIFIERS = {
		"facebook-light" : "light",
		"facebook-dark" : "dark",
	}


##############################################################################################################################
##############################################################################################################################
### AWS
##############################################################################################################################
##############################################################################################################################

# Load up the necessary AWS infrastructure
# Note: On remote infrastructures, we don't authenticate as the Lambda handler will have the necessary
# permissions built into it
AWS_REQUIRED_RESOURCES = ["s3"]

AWS_VIDEO_TO_IMGS_ARN = "arn:aws:lambda:ap-southeast-2:519969025508:function:moat_video_to_imgs"
ARN_SELF = "arn:aws:lambda:ap-southeast-2:519969025508:function:moat_rdo_constructor"

META_ADLIBRARY_CONFIG = {
		"aws" : {
			"AWS_PROFILE" : "dmrc",
			"AWS_REGION" : "ap-southeast-2"
		},
		"observations_bucket" : "fta-mobile-observations-v2"
	}
S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"
S3_MOBILE_OBSERVATIONS_CCL_BUCKET = "fta-mobile-observations-v2-ccl"

def aws_load(running_locally=False):
	credentials_applied = dict()
	if (running_locally):
		# Running locally
		credentials = boto3.Session(profile_name=META_ADLIBRARY_CONFIG["aws"]["AWS_PROFILE"]).get_credentials()
		credentials_applied = {
				"region_name" : META_ADLIBRARY_CONFIG["aws"]["AWS_REGION"],
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


def quick_access_cache_read(this_observer_uuid):
	return json.loads(AWS_RESOURCE["s3"].Object(META_ADLIBRARY_CONFIG["observations_bucket"], 
		f"{this_observer_uuid}/quick_access_cache.json").get()['Body'].read())

def quick_access_cache_write(this_observer_uuid, quick_access_cache):
	AWS_RESOURCE["s3"].Object(META_ADLIBRARY_CONFIG["observations_bucket"], 
		f'{this_observer_uuid}/quick_access_cache.json').put(Body=json.dumps(quick_access_cache, indent=3))

template_cache = dict()

def cache_exists(this_observer_uuid, cache_name="quick_access_cache"):
	try:
		AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/{cache_name}.json").get()['Body'].read()
		return True
	except:
		return False

def cache_read(this_observer_uuid, cache_name="quick_access_cache", template_cache=template_cache):
	try:
		return json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/{cache_name}.json").get()['Body'].read())
	except:
		return template_cache

def cache_write(this_observer_uuid, cache=template_cache, cache_name="quick_access_cache"):
	AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/{cache_name}.json').put(Body=json.dumps(cache, indent=3))



def s3_object_exists(this_bucket, this_path):
	try:
		AWS_CLIENT["s3"].head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False

'''
	This function determines the contents of a subbucket within an S3 bucket
'''
def subbucket_contents(kwargs, search_criteria="CommonPrefixes"):
	global AWS_CLIENT
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

##############################################################################################################################
##############################################################################################################################
### UTILITY FUNCTIONS
##############################################################################################################################
##############################################################################################################################

# Note from the mass downloader lambda that all images were saved as JPEG, and all videos as MP4
ACCEPTABLE_MEDIA_TYPES = {
		"image" : "jpeg",
		"video" : "mp4"
	}

def determine_key_path_on_value(this_obj, value, trace=list()):
	this_trace = list(trace)
	if (type(this_obj) is list):
		for i in range(len(this_obj)):
			tentative_key_path = determine_key_path_on_value(this_obj[i], value, this_trace + [i])
			if (tentative_key_path is not None):
				return tentative_key_path
	elif (type(this_obj) is dict):
		for k in this_obj:
			tentative_key_path = determine_key_path_on_value(this_obj[k], value, this_trace + [k])
			if (tentative_key_path is not None):
				return tentative_key_path
	else:
		if (this_obj == value):
			return this_trace
		else:
			return None
	return None

##############################################################################################################################
##############################################################################################################################
### MAIN ROUTINE
##############################################################################################################################
##############################################################################################################################

def load_resource(observer_uuid, tentative_ad, type, output_f):
	return json.loads(AWS_RESOURCE["s3"].Object(META_ADLIBRARY_CONFIG["observations_bucket"], 
				f'{observer_uuid}/{type}/{tentative_ad}/{output_f}.json').get()['Body'].read())



'''
	
	rdo constructor wakes up - 

		isolates all observvers with formalized caches - 

		for those that do not yet have rdo construction, 

			add to execution list

		or

			for those that have rdo construction, but also have new compilations that extend beyond the original 
			rdo constructions execution time, and that can be synthesized

			also add to rdo construction list

		shuffle

		go through and construct rdo

'''

REGARDABLE_SYNTHESES = ["ccl_advertiser_scrape", "disabled_ad", "enabled_ad", "ccl_advertiser_name_extraction", "ccl_advertiser_scrape_v2_mass_download"] # TODO - as we (re)integrate asynchronous data layers, they'll appear here as keywords to flag to the batch event

CCL_PLATFORM_VENDOR_MAPPINGS = {
		"FACEBOOK" : "meta_adlibrary",
		"INSTAGRAM" : "meta_adlibrary",
		"TIKTOK" : "unknown",
		"YOUTUBE" : "unknown"
	}

def push_to_reindex(observer_uuid, rdo_path, term):
	to_reindex = cache_read("common", cache_name="to_reindex")
	if (not "reindex" in to_reindex): to_reindex["reindex"] = list()
	to_reindex["reindex"].append({"observer_uuid" : observer_uuid, "rdo_path" : rdo_path, "term" : term})
	#cache_write("common", cache=to_reindex, cache_name="to_reindex")

N_SECONDS_TIMEOUT = (60 * 14) + 30
def routine_batch(event, context=None):
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})

	#ccl_cache = json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_cache.json").get()['Body'].read())
	ccl_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				}
			})
	#ccl_data_donation_cache = json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_data_donation_cache.json").get()['Body'].read())
	ccl_data_donation_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				}
			})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	for _this_observer_uuid in observer_uuids: # observer_uuids
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		entrypoint_cache_path = f"{this_observer_uuid}/formalized_cache.json"
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)):
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			for formalized_uuid in formalized_cache:
				enacted_syntheses = [x for x in REGARDABLE_SYNTHESES if ((x in formalized_cache[formalized_uuid]) 
						and ((not "rdo" in formalized_cache[formalized_uuid]) or (formalized_cache[formalized_uuid][x] > formalized_cache[formalized_uuid]["rdo"])))]
				if ((not "rdo" in formalized_cache[formalized_uuid]) or (len(enacted_syntheses) > 0)):
						rdo_intents.append({
								"observer_uuid" : this_observer_uuid,
								"formalized_uuid" : formalized_uuid,
								"syntheses" : [x for x in formalized_cache[formalized_uuid] if (not (x == "rdo"))]
							})
	# Shuffle the RDOs that need to be executed
	random.shuffle(rdo_intents)
	if (__name__ == "__main__"):
		ipdb.set_trace()
	# Execute with early timeout if necessary
	for x in rdo_intents:
		time_at_call = int(time.time())
		print("\tExecuting observer_uuid: ", x["observer_uuid"], " formalized_uuid: ", x["formalized_uuid"])
		routine_instance(x | {
				"ccl_cache" : ccl_cache,
				"ccl_data_donation_cache" : ccl_data_donation_cache
			})
		print("\t\tElapsed time: ", abs(time_at_call - int(time.time())), " seconds")
		elapsed_time = abs(int(time.time()) - time_at_init)

		if ((__name__ != "__main__") and (elapsed_time > N_SECONDS_TIMEOUT)):
			break
	return str()

def routine_instance(rdo_intent, context=None):
	pass_this = False
	metadata_dict = dict() ##
	frame_ocr_dict = dict() ##
	observer_uuid = rdo_intent["observer_uuid"]

	# Load in the disabled ads path (if it exists)
	disabled_ads_path = f'{observer_uuid}/disabled_ads.json'
	disabled_ads = list()
	if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path)):
		disabled_ads = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path).get()['Body'].read())
	disabled_ads = [x.split(".")[1] for x in disabled_ads]
	is_user_disabled = (rdo_intent["formalized_uuid"] in disabled_ads)
		
	try:
		formalized_obj = json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
					f'{observer_uuid}/formalized/{rdo_intent["formalized_uuid"]}.json').get()['Body'].read()) ##
	except:
		print("ABSENT KEY ERROR!!!!") #TODO
		return str()
	# Retrieve the user demographic characteristics (if they exist)
	demographic_characteristics = None
	try: demographic_characteristics = json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{observer_uuid}/misc/demographic_characteristics.json').get()['Body'].read()) ##
	except: pass
	demographic_data_AUFedEl_2025 = json.loads(open('demographic_data_AUFedEl_2025.json').read())
	#observer_uuid[-7:-1]
	this_activation_code = observer_uuid[-7:-1].upper()
	try: demographic_characteristics = [x for x in demographic_data_AUFedEl_2025 if (this_activation_code in x["activation_codes"])][0]
	except: pass

	#
	rdo_path = f'{int(formalized_obj[0]["frame_observed_at"]*1000)}.{rdo_intent["formalized_uuid"]}'
	for x in formalized_obj:
		data_donation_uuid = x["data_donation_uuid"]
		metadata_path = f"{observer_uuid}/temp-v2/{data_donation_uuid}/metadata.json"
		if (not data_donation_uuid in metadata_dict):
			metadata_dict[data_donation_uuid] = json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, metadata_path).get()['Body'].read())
		frame_ocr_path = f'{observer_uuid}/temp-v2/{data_donation_uuid}/{x["frame"]}.jpg.ocr.json'
		frame_ocr_dict[x["frame"]] = json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, frame_ocr_path).get()['Body'].read())
	system_info = metadata_dict[list(metadata_dict.keys())[0]]["nameValuePairs"]["systemInformation"]["internalJSONObject"]["nameValuePairs"]

	recording_info = None
	recording_info_formalized = { "n_seconds": float(), "n_frames": int(), "fps": float() }
	try:
		recording_info = metadata_dict[list(metadata_dict.keys())[0]]["nameValuePairs"]["recordingInformation"]["internalJSONObject"]["nameValuePairs"]
		recording_info_formalized = {
				"fps" : recording_info["FPS"],
				"n_frames" : recording_info["nFrames"],
				"n_seconds" : recording_info["durationInMilliseconds"] / 1000,
			}
	except:
		print("RECORDING INFO BLOCK:")
		print("observer_uuid: ", observer_uuid)
		print("rdo_path: ", rdo_path)
		print("formalized_uuid: ", rdo_intent["formalized_uuid"])
		print(traceback.format_exc())
		push_to_reindex(observer_uuid, rdo_path, "RECORDING_INFO")
		pass_this = True
	entrypoint_cache = json.loads(AWS_RESOURCE["s3"].Object(
		S3_BUCKET_MOBILE_OBSERVATIONS, f'{rdo_intent["observer_uuid"]}/entrypoint_cache.json').get()['Body'].read())

	# Joined At
	joined_at_value = None
	try:
		joined_at_path = f'{rdo_intent["observer_uuid"]}/joined_at.json'
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, joined_at_path)):
			# Take the joined at value
			joined_at_value = int(json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, joined_at_path).get()['Body'].read())["joined_at_raw"])/1000
		else:
			# Otherwise if it doesn't exist, take the smallest formalized value value
			data_donation_observations = list()
			for k in entrypoint_cache:
				this_observation_metadata = json.loads(AWS_RESOURCE["s3"].Object(
					S3_BUCKET_MOBILE_OBSERVATIONS, f'{rdo_intent["observer_uuid"]}/temp-v2/{k}/metadata.json').get()['Body'].read())
				data_donation_observations.append(this_observation_metadata["nameValuePairs"]["observedAt"])
			joined_at_value = int(min(data_donation_observations))
	except:
		print("JOINED AT BLOCK:")
		print("observer_uuid: ", observer_uuid)
		print("rdo_path: ", rdo_path)
		print("formalized_uuid: ", rdo_intent["formalized_uuid"])
		print(traceback.format_exc())
		push_to_reindex(observer_uuid, rdo_path, "JOINED_AT")
		pass_this = True
	# Note: Legacy TikTok entries don't feature ad types
	try:
		ad_type = metadata_dict[list(metadata_dict.keys())[0]]["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][str(formalized_obj[0]["frame"])]["internalJSONObject"]["nameValuePairs"]["adType"]
	except:
		ad_type = "UNKNOWN"




	# CCL-related stuff
	ccl_content = dict()
	ccl_v2_content = dict()
	ccl_media_mappings = dict()

	ccl_legacy_meta_adlibrary_content_candidates = list()
	ccl_legacy_meta_adlibrary_query = { "restitched_image_key": "unknown", "value": "unknown", "confidence": 0.0 }
	ccl_legacy_meta_adlibrary_ad_scrape_comparisons = list()
	ccl_legacy_meta_adlibrary_ad_scrape_sources = dict()
	#
	try:
		ccl_path = f'{rdo_intent["observer_uuid"]}/ccl/{rdo_path}'
		ccl_advertiser_name_extraction = None
		has_ccl_advertiser_name_extraction = ("ccl_advertiser_name_extraction" in rdo_intent["syntheses"])
		has_ccl_advertiser_scrape = ("ccl_advertiser_scrape" in rdo_intent["syntheses"])
		has_ccl_advertiser_scrape_v2 = ("ccl_advertiser_scrape_v2_mass_download" in rdo_intent["syntheses"])

		# CCL V2
		if (has_ccl_advertiser_name_extraction):

			# Trace backwards - start at the entrypoint_cache, and locate all data donations with the formalized_uuid
			data_donation_uuids = list()
			for this_data_donation_uuid in entrypoint_cache:
				if (("formalized_v2_uuids" in entrypoint_cache[this_data_donation_uuid]) 
					and (rdo_intent["formalized_uuid"] in entrypoint_cache[this_data_donation_uuid]["formalized_v2_uuids"])):
					data_donation_uuids.append(this_data_donation_uuid)
			# Then using the data donation uuids, go to the data donation cache, and find all group uuids associated with the data donation uuids
			relevant_ccl_cache_uuids = list()
			grouped_terms_dict = dict()
			for this_data_donation_uuid in data_donation_uuids:
				try:
					ccl_data_donation_cache_entry = rdo_intent["ccl_data_donation_cache"][f"{rdo_intent['observer_uuid']}/{this_data_donation_uuid}.json"]
				except:
					# In this case, the CCL identification is malformed and needs to be corrected - this is done by reverting the entrypoint cache to reflect
					# that no CCL identification is present - furthermore, the formalized cache must also remove the ccl_advertiser_name_extraction
					entrypoint_cache = cache_read(rdo_intent["observer_uuid"], cache_name="entrypoint_cache")
					# Remove from the entrypoint_cache
					if ("ccl_terms_identified_v4" in entrypoint_cache):
						del entrypoint_cache[this_data_donation_uuid]["ccl_terms_identified_v4"]
						cache_write(rdo_intent["observer_uuid"], cache=entrypoint_cache, cache_name="entrypoint_cache")
					# Remove the indication from the formalized cache
					formalized_cache = cache_read(observer_uuid, cache_name="formalized_cache")
					if ("ccl_advertiser_name_extraction" in formalized_cache[rdo_intent["formalized_uuid"]]):
						del formalized_cache[rdo_intent["formalized_uuid"]]["ccl_advertiser_name_extraction"]
						cache_write(observer_uuid, cache=formalized_cache, cache_name="formalized_cache")
					print("Early exit on missing ccl_advertiser_name_extraction")
					return str()


				relevant_ccl_cache_uuids.extend(ccl_data_donation_cache_entry["group_term_uuids"])
				if (not ccl_data_donation_cache_entry["group_uuid"] in grouped_terms_dict):
					grouped_terms_dict[ccl_data_donation_cache_entry["group_uuid"]] = list()
				grouped_terms_dict[ccl_data_donation_cache_entry["group_uuid"]].append(this_data_donation_uuid)
			relevant_ccl_cache_uuids = list(set(relevant_ccl_cache_uuids))
			ipdb.set_trace()
			ccl_cache_records = dict()
			for this_ccl_cache_uuid in relevant_ccl_cache_uuids:
				tentative_record = dict(rdo_intent["ccl_cache"][this_ccl_cache_uuid])
				tentative_record["data_donation_uuids"] = grouped_terms_dict[tentative_record["group_uuid"]]
				ccl_cache_records[this_ccl_cache_uuid] = tentative_record
				grouped_terms = json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, 
					f"grouped_terms/{rdo_intent['observer_uuid']}/{tentative_record['group_uuid']}.json").get()['Body'].read())
				grouped_terms_i = grouped_terms[tentative_record["group_i"]]
				for k in ["similarities", "offset_map", "reweighted_term_characters"]:
					del grouped_terms_i[k]
				tentative_record["group_terms"] = grouped_terms_i
				tentative_record["vendor"] = CCL_PLATFORM_VENDOR_MAPPINGS[tentative_record["platform"]].upper()
			ccl_v2_content |= ccl_cache_records
		if (has_ccl_advertiser_scrape_v2):
			# We can use the advertiser name extractoins as the basis for this part...
			base_record_uuid = "a41ddec3-870e-4f64-a7ce-3257a6d738e1"
			for base_record_uuid in ccl_v2_content:
				if (ccl_v2_content[base_record_uuid]["vendor"] == "META_ADLIBRARY"):
					applied_record_uuid = base_record_uuid
					if (not "outcome" in ccl_v2_content[base_record_uuid]):
						print("Early exit on failed alias (most likely due to rollback...)")
						return str()
					if (ccl_v2_content[base_record_uuid]["outcome"]["status"] == "ALIASED"):
						applied_record_uuid = ccl_v2_content[base_record_uuid]["outcome"]["alias_uuid"]
						ccl_v2_content[base_record_uuid]["alias"] = rdo_intent["ccl_cache"][applied_record_uuid]
						if (not "outcome" in ccl_v2_content[base_record_uuid]["alias"]):
							print("Early exit on failed alias (most likely due to rollback...)")
							return str()
						del ccl_v2_content[base_record_uuid]["alias"]["outcome"]
					base_path = f"outputs/meta_adlibrary/meta_adlibrary_scrapes/{applied_record_uuid}"
					if (not ccl_v2_content[base_record_uuid]["outcome"]["status"] in ["SCRAPED", "ALIASED"]):
						ccl_v2_content[base_record_uuid]["deliberate_failed_scrape_details"] = dict()
						for k in ["status", "comment"]:
							if (k in ccl_v2_content[base_record_uuid]["outcome"]):
								ccl_v2_content[base_record_uuid]["deliberate_failed_scrape_details"][k] = ccl_v2_content[base_record_uuid]["outcome"][k]
					else:
						namecheck_output = json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, f"{base_path}/namecheck_output.json").get()['Body'].read())
						scrape_output = json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, f"{base_path}/scrape_output.json").get()['Body'].read())
						# There should be a namecheck output, scrape output
						ccl_v2_content[base_record_uuid]["namecheck_output"] = namecheck_output
						ccl_v2_content[base_record_uuid]["scrape_output"] = scrape_output
		# TODO - wipe outcomes from ccl_v2_content after all foresteps
		for k in ccl_v2_content:
			if ("outcome" in ccl_v2_content[k]):
				del ccl_v2_content[k]["outcome"]
		#print(json.dumps(ccl_v2_content,indent=3))
		#ipdb.set_trace()


		#if (has_ccl_advertiser_name_extraction and has_ccl_advertiser_scrape):
		ccl_advertiser_scrape_path = f'{ccl_path}/advertiser_name_extraction.json'
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, ccl_advertiser_scrape_path)):
			ccl_advertiser_name_extraction = json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, ccl_advertiser_scrape_path).get()['Body'].read())
			advertiser_names = ccl_advertiser_name_extraction["result"]
			ccl_content["advertiser_name_extractions"] = advertiser_names
		if (has_ccl_advertiser_scrape):
			try:
				if (CCL_PLATFORM_VENDOR_MAPPINGS[ccl_advertiser_name_extraction["platform"]] == "meta_adlibrary"): # TODO - remove
					ccl_scrape_instances = json.loads(AWS_RESOURCE["s3"].Object(
						S3_BUCKET_MOBILE_OBSERVATIONS, f'{ccl_path}/scrape_instances.json').get()['Body'].read())
					ccl_content["scrapes"] = list()
					for x in ccl_scrape_instances:
						scrape_output_path = f'{ccl_path}/{x["scrape_instance_uuid"]}/scrape_output.json'
						scrape_instance_media_path = f'{rdo_intent["observer_uuid"]}/ccl/{rdo_path}/{x["scrape_instance_uuid"]}/medias/'
						scrape_metadata_path = f'{ccl_path}/{x["scrape_instance_uuid"]}/medias/metadata.json'
						if ("alias" in x):
							calling_observer_uuid = x["alias"]["this_observer_uuid"]
							calling_rdo_path = x["alias"]["rdo_uuid_unsplit"]
							calling_scrape_instance_uuid = x["alias"]["scrape_instance_uuid"]
							calling_ccl_path = f'{calling_observer_uuid}/ccl/{calling_rdo_path}'
							scrape_output_path = f'{calling_ccl_path}/{calling_scrape_instance_uuid}/scrape_output.json'
							scrape_instance_media_path = f'{calling_observer_uuid}/ccl/{calling_rdo_path}/{calling_scrape_instance_uuid}/medias/'
							scrape_metadata_path = f'{calling_ccl_path}/{calling_scrape_instance_uuid}/medias/metadata.json'
						ccl_scrape_output = json.loads(AWS_RESOURCE["s3"].Object(
							S3_BUCKET_MOBILE_OBSERVATIONS, scrape_output_path).get()['Body'].read())
						ccl_scrape_media_metadata = {"download_log" : list()}
						if (len(ccl_scrape_output["response_interpreted"]["outlinks"]) != 0):
							ccl_scrape_media_metadata = json.loads(AWS_RESOURCE["s3"].Object(
								S3_BUCKET_MOBILE_OBSERVATIONS, scrape_metadata_path).get()['Body'].read())
							for this_media_uuid in ccl_scrape_media_metadata["download_log"]:
								real_s3_key = f'{scrape_instance_media_path}/{this_media_uuid}'
								ccl_media_mappings[ccl_scrape_media_metadata["download_log"][this_media_uuid]["url"]] = real_s3_key
							
						this_scrape_instance = {
							"uuid" : x["scrape_instance_uuid"],
							"vendor" : CCL_PLATFORM_VENDOR_MAPPINGS[ccl_advertiser_name_extraction["platform"]].upper(),
							"query" : {
								"value" : x["query"],
								"aggregated_from" : [advertiser_names[y]["advertiser_name"] for y in x["aggregates"]]
							},
							"response" : ccl_scrape_output,
							"medias" : ccl_scrape_media_metadata["download_log"]
						}
						if ("alias" in x):
							this_scrape_instance["alias"] = x["alias"]
						# Legacy block
						if (CCL_PLATFORM_VENDOR_MAPPINGS[ccl_advertiser_name_extraction["platform"]] == "meta_adlibrary"):
							ccl_legacy_meta_adlibrary_content_candidates.extend(ccl_scrape_output["response_interpreted"]["json_interpreted"])
							if (ccl_legacy_meta_adlibrary_query["value"] == "unknown"):
								ccl_legacy_meta_adlibrary_query["value"] = x["query"]
							else:
								ccl_legacy_meta_adlibrary_query["value"] += " OR "+x["query"]
						ccl_content["scrapes"].append(this_scrape_instance)
			except:
				pass


		# Legacy block
		if (ccl_advertiser_name_extraction is not None):
			try:
				if (CCL_PLATFORM_VENDOR_MAPPINGS[ccl_advertiser_name_extraction["platform"]] == "meta_adlibrary"):
					ccl_legacy_meta_adlibrary_query["confidence"] = statistics.mean([x["confidence"] for x in ccl_content["advertiser_name_extractions"]])
					for i in range(len(ccl_legacy_meta_adlibrary_content_candidates)):
						ccl_legacy_meta_adlibrary_content_candidates[i] = { 
								"ad_library_scrape_candidates_i" : i, 
								"data" : ccl_legacy_meta_adlibrary_content_candidates[i] 
							}
						try:
							this_media_url = ccl_legacy_meta_adlibrary_content_candidates[i]["data"]["snapshot"]["cards"][0]["resized_image_url"]
							this_scrape_instance_uuid = None
							this_media_uuid = None
							for x in ccl_content["scrapes"]:
								if (this_media_uuid is None):
									for media_uuid in x["medias"]:
										if (this_media_uuid is None):
											if (x["medias"][media_uuid]["url"] == this_media_url):
												this_media_uuid = media_uuid
												this_scrape_instance_uuid = x["uuid"]
							if (this_media_uuid is not None):
								real_s3_key = f'{rdo_intent["observer_uuid"]}/ccl/{rdo_path}/{this_scrape_instance_uuid}/medias/{this_media_uuid}'
								ccl_legacy_meta_adlibrary_ad_scrape_comparisons.append({
										"restitched_image_key" : str(),
										"scrape_media_key" : real_s3_key,
										"result" : {"coverage_pct" : float(), "similarity_pct" : float(), "comparison_pixel_n_samples" : int()}
									})
								ccl_legacy_meta_adlibrary_ad_scrape_sources[this_media_uuid] = {
									"key_path" : list(),
									"media_url" : this_media_url,
									"real_s3_key" : real_s3_key,
									"ad_library_scrape_candidates_i" : i
								}
						except:
							pass
						# Go through the data of the ad scrape and identify if any urls are those present within the metadata
			except:
				pass

	except:
		print("CCL BLOCK:")
		print("observer_uuid: ", observer_uuid)
		print("rdo_path: ", rdo_path)
		print("formalized_uuid: ", rdo_intent["formalized_uuid"])
		print(traceback.format_exc())
		push_to_reindex(observer_uuid, rdo_path, "CCL")
		pass_this = True

	medias = list()
	keyframes = list()
	for x in formalized_obj:
		frame_img_path = f'{observer_uuid}/temp-v2/{x["data_donation_uuid"]}/{x["frame"]}.jpg'
		medias.append(frame_img_path)
		keyframes.append({ 
				"observed_at" : x["observed_at"],  
				"y_offset" : 0, # legacy
				"screenshot_cropped" : frame_img_path,
				"frame" : frame_img_path,
				"ocr_data" : frame_ocr_dict[x["frame"]]
			})
	output = {
		"version" : 2.0,
		"is_user_disabled" : is_user_disabled,
		"observer" : {
				"uuid" : observer_uuid,
				"demographic_characteristics" : demographic_characteristics,
				"joined_at" : joined_at_value,
				"device_dimensions" : system_info["screenDimensions"]["internalJSONObject"]["nameValuePairs"], # legacy
				"device" : {
					"dimensions" : system_info["screenDimensions"]["internalJSONObject"]["nameValuePairs"],
					"os_version" : system_info["operatingSystemVersion"],
					"api_level" : system_info["apiLevel"],
					"device" : system_info["device"],
					"model" : system_info["model"]
				}
			},
		"observation" : {
			"uuid" : rdo_path,
			"observed_on_device_at" : int(formalized_obj[0]["frame_observed_at"]), # legacy
			"observed_on_device_at_decimal" : formalized_obj[0]["frame_observed_at"], # legacy
			"submitted_from_device_at" : min([metadata_dict[k]["nameValuePairs"]["preparedAt"] for k in metadata_dict]), # legacy
			"platform" : metadata_dict[list(metadata_dict.keys())[0]]["nameValuePairs"]["platform"],
			"ad_format" : ad_type,
			"exposure" : "unknown", # legacy
			"media_bounds" : list(), # legacy
			"whitespace_derived_color" : "unknown", # legacy
			"whitespace_derived_signature" : list(), # legacy
			"ad_dimensions" : system_info["screenDimensions"]["internalJSONObject"]["nameValuePairs"], # legacy
			"video" : recording_info_formalized,
			"keyframes" : keyframes
		},
		"enrichment" : {
			"ccl_v2" : ccl_v2_content,
			"ccl" : ccl_content,
			"meta_adlibrary_scrape": {
				"comparisons": {
					"ocr": [],
					"image": {
						"comparisons": ccl_legacy_meta_adlibrary_ad_scrape_comparisons,
						"ad_scrape_sources": ccl_legacy_meta_adlibrary_ad_scrape_sources
					},
					"video": {
						"comparisons": [],
						"ad_scrape_sources": {}
					}
				},
				"candidates": ccl_legacy_meta_adlibrary_content_candidates,
				"rankings": [],
				"scraped_at": 0,
				"reference": {
					"caller": {
						"observer_uuid": "unknown",
						"tentative_ad": "unknown"
					},
					"scrape": {
						"observer_uuid": "unknown",
						"tentative_ad": "unknown"
					}
				},
				"query": ccl_legacy_meta_adlibrary_query
			},
			"media" : ccl_media_mappings
		},
		"media" : medias
	}
	if (not pass_this):
		# Put the object in place
		AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{observer_uuid}/rdo/{rdo_path}/output.json').put(Body=json.dumps(output, indent=3))
		# Update the cache
		formalized_cache = cache_read(observer_uuid, cache_name="formalized_cache")
		formalized_cache[rdo_intent["formalized_uuid"]]["rdo"] = int(time.time()) # set to last execution
		cache_write(observer_uuid, cache=formalized_cache, cache_name="formalized_cache")
		# Update the quick_access_cache
		quick_access_cache = cache_read(observer_uuid, cache_name="quick_access_cache")
		if (not ("ads_passed_rdo_construction" in quick_access_cache)):
			quick_access_cache["ads_passed_rdo_construction"] = list()
		quick_access_cache["ads_passed_rdo_construction"].append(f"{observer_uuid}/temp/{rdo_path}/")
		cache_write(observer_uuid, cache=quick_access_cache, cache_name="quick_access_cache")
	return str()

def routine_target(event, context=None):
	rdo_intents = list()
	time_at_init = int(time.time())
	this_observer_uuid = event["observer_uuid"]
	print(this_observer_uuid)
	entrypoint_cache_path = f"{this_observer_uuid}/formalized_cache.json"
	if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)):
		formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
		for formalized_uuid in formalized_cache:
			enacted_syntheses = [x for x in REGARDABLE_SYNTHESES if ((x in formalized_cache[formalized_uuid]) 
					and (formalized_cache[formalized_uuid][x] > formalized_cache[formalized_uuid]["rdo"]))]
			if ((not "rdo" in formalized_cache[formalized_uuid]) or (len(enacted_syntheses) > 0)):
					rdo_intents.append({
							"observer_uuid" : this_observer_uuid,
							"formalized_uuid" : formalized_uuid,
							"syntheses" : [x for x in formalized_cache[formalized_uuid] if (not (x == "rdo"))],
							"formalized_cache" : formalized_cache
						})
	# Shuffle the RDOs that need to be executed
	random.shuffle(rdo_intents)
	# Execute with early timeout if necessary
	for x in rdo_intents:
		time_at_call = int(time.time())
		print("\tExecuting observer_uuid: ", x["observer_uuid"], " formalized_uuid: ", x["formalized_uuid"])
		routine_instance(x)
		print("\t\tElapsed time: ", abs(time_at_call - int(time.time())), " seconds")
		elapsed_time = abs(int(time.time()) - time_at_init)

		if ((__name__ != "__main__") and (elapsed_time > N_SECONDS_TIMEOUT)):
			break

processes = { "batch" : routine_batch, "routine_instance" : routine_instance, "routine_target" : routine_target }



def repair():
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/formalized_cache.json")):
			print("Applying to ", this_observer_uuid)
			# Empty the RDO bucket
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
			if ("Contents" in response):
				for x in response['Contents']:
					#print(x)
					if (x['Key'].split("/")[2].split(".")[-1] in formalized_cache):
						print('Deleting', x['Key'])
						AWS_CLIENT["s3"].delete_object(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Key=x['Key'])
			# flush the formalized_cache
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			formalized_cache = {k:dict() for k in formalized_cache}
			cache_write(this_observer_uuid, cache=formalized_cache, cache_name="formalized_cache")
			# flush the quick_access_cache
			quick_access_cache = cache_read(this_observer_uuid, cache_name="quick_access_cache")
			quick_access_cache["ads_passed_rdo_construction"] = list()
			cache_write(this_observer_uuid, cache=quick_access_cache, cache_name="quick_access_cache")

'''
	Find an RDO with an unknown ad type
'''
def test_event():
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
		if ("Contents" in response):
			for x in response['Contents']:
				# Read in the object
				this_rdo = json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, x["Key"]).get()['Body'].read()) ##
				if (("ad_format" in this_rdo["observation"]) and (this_rdo["observation"]["ad_format"] == "UNKNOWN")):
					print(x["Key"])
					print(this_rdo["observation"]["ad_format"])

'''
	Find an RDO with an unknown ad type
'''
def test_event_check_joined_at():
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	i = 0
	j = 0
	k = 0
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print("this_observer_uuid")
		i += 1
		response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
		if ("Contents" in response):
			k += 1
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/joined_at.json")):
			print("\tHAS")
			j += 1
		else:
			print("\t DOESNT HAVE")
	ipdb.set_trace()

'''
	Find an RDO with an unknown ad type
'''
def test_event_check_joined_at():
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	i = 0
	j = 0
	k = 0
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		i += 1
		response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
		if ("Contents" in response):
			k += 1
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/joined_at.json")):
			print("\tHAS")
			j += 1
		else:
			print("\t DOESNT HAVE")
	ipdb.set_trace()


def test_event_check_n_ads():
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	k = 0
	for _this_observer_uuid in ["ff38b2c5-c7b8-4308-ba35-720225b38679"]:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
		if ("Contents" in response):
			#if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/joined_at.json")):
			k += len([x for x in response['Contents']])
			print(k)
	ipdb.set_trace()

def test_event_check_constants():
	zzz = list()
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	k = 0
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print("this_observer_uuid: " + this_observer_uuid)
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/joined_at.json")):
			response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
			if ("Contents" in response):
				print("\t ...has contents...")
				for x in response['Contents']:
					if ("output.json" in x["Key"]):
						print(x["Key"])
						try:
							this_rdo = json.loads(AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS, x["Key"]).get()['Body'].read()) ##
							zzz.append("_".join([str(z) for z in [this_rdo["observer"]["device"]["os_version"],
								this_rdo["observer"]["device"]["api_level"],
								this_rdo["observer"]["device"]["device"],
								this_rdo["observer"]["device"]["model"]]]))
							break
						except: 
							print(traceback.format_exc())
							#ipdb.set_trace()
							pass#eipdb.set_trace()
					else:
						break

	ipdb.set_trace()

def test_event_check_reindex():
	to_reindex_distinct = json.loads(AWS_RESOURCE["s3"].Object(
		S3_BUCKET_MOBILE_OBSERVATIONS, "common/to_reindex.json").get()['Body'].read())
	paths_ccl = list()
	paths_other = list()
	for x in to_reindex_distinct["reindex"]:
		this_path = f'{x["observer_uuid"]}/ccl/{x["rdo_path"]}'
		if ((x["term"] == "CCL") and (not this_path in paths_ccl)):
			paths_ccl.append(this_path)
		if ((x["term"] != "CCL") and (not this_path in paths_other)):
			paths_other.append(this_path)

	passed = list()
	to_index = list()
	for _this_observer_uuid in subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS}):
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/formalized_cache.json")):
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			for formalized_uuid in formalized_cache:
				if ("rdo" in formalized_cache[formalized_uuid]):
					if (formalized_cache[formalized_uuid]["rdo"] == max(formalized_cache[formalized_uuid].values())):
						passed.append({"observer_uuid" : this_observer_uuid, "formalized_uuid" : formalized_uuid})
				if ("ccl_advertiser_scrape" in formalized_cache[formalized_uuid]):
					if (formalized_cache[formalized_uuid]["ccl_advertiser_scrape"] == max(formalized_cache[formalized_uuid].values())):
						to_index.append({"observer_uuid" : this_observer_uuid, "formalized_uuid" : formalized_uuid})


	print("CCL to reindex:", len(paths_ccl))
	print("OTHER to reindex:", len(paths_other))
	print("passed:", len(passed))
	print("to_index:", len(to_index))
	with open(os.path.join(os.getcwd(),"rdo_ccl_reindex.json"), "w") as f: f.write(json.dumps(paths_ccl, indent=3))
	with open(os.path.join(os.getcwd(),"rdo_other_reindex.json"), "w") as f: f.write(json.dumps(paths_other, indent=3))
	with open(os.path.join(os.getcwd(),"rdo_passed.json"), "w") as f: f.write(json.dumps(passed, indent=3))
	ipdb.set_trace()

def test_event_force_rdo_ccl_scrape_completions():
	for _this_observer_uuid in subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS}):
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/formalized_cache.json")):
			formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache")
			for formalized_uuid in formalized_cache:
				if ("ccl_advertiser_scrape" in formalized_cache[formalized_uuid]):
					formalized_cache[formalized_uuid]["ccl_advertiser_scrape"] = int(time.time())
				print(f"UPDATED {this_observer_uuid}/{formalized_uuid}")
			cache_write(this_observer_uuid, cache=formalized_cache, cache_name="formalized_cache")


def test_event_get_rdos():
	# Get all observer UUIDs
	totals = list()
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
		if ("Contents" in response):
			for x in response['Contents']:
				totals.append(x["Key"])
		print(len(totals))

def lambda_handler(event, context):
	response_body = dict()

	# Evaluate the action
	if ("action" in event):
		if (event["action"] in processes):
			print("Action: ", event["action"])
			response_body = processes[event["action"]](event, context)

	return {
		'statusCode': 200,
		'body': json.dumps(response_body)
	}

def test_event_check_joined_at_after_date():
	rdo_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	ii = int()
	qualified_observer_uuids = list()
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print("this_observer_uuid")
		response = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{this_observer_uuid}/rdo/")
		if ("Contents" in response):
			if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/joined_at.json")):
				joined_at = (int(json.loads(AWS_RESOURCE["s3"].Object(
					S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/joined_at.json").get()['Body'].read())["joined_at_raw"])/1000)
				if (joined_at > 1749654000):
					print(ii)
					ii += 1
	ipdb.set_trace()

'''

	get device ID and reg code for each user

	next determine the reg codes that are accounted for by octopus and those that are not - for registration since april

'''
if (__name__ == "__main__"):
	#routine_batch(dict())
	#ipdb.set_trace()
	
	'''

   "f30aeafd-02f7-4b45-8c6e-75ae544cd679": {
      "uuid": "f30aeafd-02f7-4b45-8c6e-75ae544cd679",
      "term": "whoiselijahparfum",
      "group_i": 2,
      "platform": "INSTAGRAM",
      "ad_type": "FEED_BASED",
      "observer_uuid": "1cf98df3-fa71-4074-9dcc-e95a222f51b3",
      "group_uuid": "bd6c77c0-e9f9-4d4a-bcae-4428ea95bb5e",
      "timestamp": 1750416548.9310346,
      "outcome": {
         "at": 1758851443,
         "version": 1000,
         "status": "ALIASED",
         "alias_uuid": "2f4e4419-38e5-49fd-befc-4e11e4fbd0f5"
      }
   },
	'''
	local_cache_path = os.path.join(os.getcwd(), "local_cache")
	try: os.mkdir(local_cache_path)
	except: pass
	local_data = dict()

	local_data["ccl_cache"] = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_cache_distributed"
				}
			})

	local_data["ccl_data_donation_cache"] = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				}
			})

	routine_instance({
			"observer_uuid": "48734387-5226-4e96-b97c-2d0747a99898",
			"formalized_uuid": "23dd6ba2-e1b2-4fe7-b789-f5030645991c",
			"syntheses": [
				"ccl_advertiser_name_extraction"
			]
		} | local_data)
	ipdb.set_trace()
	'''
	# SCRAPED CASE
	routine_instance({
			"observer_uuid" : "0c719680-7eee-43b2-bbbb-54090f05ccd6",
			"formalized_uuid" : "020f7c06-656f-4908-94f9-bafe5c95c8f1",
			"syntheses" : ["ccl_advertiser_name_extraction", "ccl_advertiser_scrape_v2_mass_download"],
			"formalized_cache" : json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, 
				"0c719680-7eee-43b2-bbbb-54090f05ccd6/formalized_cache.json").get()['Body'].read()),
			"ccl_cache" : json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_cache.json").get()['Body'].read()),
			"ccl_data_donation_cache" : json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_data_donation_cache.json").get()['Body'].read())
		})

	# ALIASED CASE
	routine_instance({
			"observer_uuid" : "1cf98df3-fa71-4074-9dcc-e95a222f51b3",
			"formalized_uuid" : "23c8a87a-5ad2-4080-9077-71fbdac540c9",
			"syntheses" : ["ccl_advertiser_name_extraction", "ccl_advertiser_scrape_v2_mass_download"],
			"formalized_cache" : json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, 
				"1cf98df3-fa71-4074-9dcc-e95a222f51b3/formalized_cache.json").get()['Body'].read()),
			"ccl_cache" : json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_cache.json").get()['Body'].read()),
			"ccl_data_donation_cache" : json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_data_donation_cache.json").get()['Body'].read())
		})
	# ALIASED WITH ROLLBACK
	routine_instance({
			"observer_uuid" : "1cf98df3-fa71-4074-9dcc-e95a222f51b3",
			"formalized_uuid" : "943da572-a6e5-4179-b91e-7c554f0afae4",
			"syntheses" : ["ccl_advertiser_name_extraction", "ccl_advertiser_scrape_v2_mass_download"],
			"formalized_cache" : json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, 
				"1cf98df3-fa71-4074-9dcc-e95a222f51b3/formalized_cache.json").get()['Body'].read()),
			"ccl_cache" : json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_cache.json").get()['Body'].read()),
			"ccl_data_donation_cache" : json.loads(AWS_RESOURCE["s3"].Object(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, "ccl_data_donation_cache.json").get()['Body'].read())
		})
	'''
	ipdb.set_trace()
	test_event_get_rdos()
	#ipdb.set_trace()
	#routine_batch(dict())
	#ipdb.set_trace()
	#test_event_check_n_ads()
	'''
	routine_instance({
			"observer_uuid" : "f3359714-57ee-4b5d-b238-217eabb86f9c",
			"formalized_uuid" : "d0b0e5e6-b0d3-42c1-b751-32b759ca43b5",
			"syntheses" : ["ccl_advertiser_name_extraction", "ccl_advertiser_scrape"],
			"formalized_cache" : json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, 
				"f3359714-57ee-4b5d-b238-217eabb86f9c/formalized_cache.json").get()['Body'].read())
		})

	'''
	'''
	routine_instance({
			"observer_uuid" : "016bdde5-b1ed-4d58-806a-24968c6746d3",
			"formalized_uuid" : "32bb487c-4cc2-4951-9982-266fd626602b",
			"syntheses" : ["ccl_advertiser_name_extraction", "ccl_advertiser_scrape"],
			"formalized_cache" : json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, 
				"016bdde5-b1ed-4d58-806a-24968c6746d3/formalized_cache.json").get()['Body'].read())
		})
	'''
	'''

	
	
	routine_instance({
			"observer_uuid" : "447745c4-2cbc-4c7b-872e-454d953ce65a",
			"formalized_uuid" : "6e49c7a8-df11-4f5d-8696-d48498ebee64",
			"syntheses" : ["ccl_advertiser_name_extraction", "ccl_advertiser_scrape"],
			"formalized_cache" : json.loads(AWS_RESOURCE["s3"].Object(
				S3_BUCKET_MOBILE_OBSERVATIONS, 
				"447745c4-2cbc-4c7b-872e-454d953ce65a/formalized_cache.json").get()['Body'].read())
		})
	'''
	test_event_check_joined_at_after_date()
	ipdb.set_trace()
	test_event_check_reindex()
	#test_event_force_rdo_ccl_scrape_completions()
	ipdb.set_trace()