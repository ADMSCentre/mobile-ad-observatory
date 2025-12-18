import sys
import os
import time
if (__name__ == "__main__"):
	import ipdb
import json
import boto3
import botocore
import traceback

AWS_REQUIRED_RESOURCES = ["s3"]

META_ADLIBRARY_CONFIG = {
		"aws" : {
			"AWS_PROFILE" : "dmrc",
			"AWS_REGION" : "ap-southeast-2"
		},
		"observations_bucket" : "fta-mobile-observations-v2"
	}
S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"
S3_BUCKET_MOBILE_OBSERVATIONS_STASIS = "fta-mobile-observations-v2-stasis"

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


PRESIGN_EXPIRY_VALUE = (3*24*60*60) # Presign expiry for json and media is 3 days and 1 hour (it must expire after the session)
def presigned_url(this_key):
	global AWS_CLIENT
	return AWS_CLIENT["s3"].generate_presigned_url(
				ClientMethod="get_object", 
				Params={ "Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, "Key" : this_key},
				ExpiresIn=PRESIGN_EXPIRY_VALUE)

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


def s3_object_exists(this_bucket, this_path):
	try:
		AWS_CLIENT['s3'].head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False

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

def process_disable_ad(event):
	disabled_ads_path = f'{event["observer_uuid"]}/disabled_ads.json'
	disabled_ads = list()
	if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path)):
		disabled_ads = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path).get()['Body'].read())
	disabled_ads.append(event["rdo_uuid_unsplit"])
	disabled_ads = list(set(disabled_ads))
	AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path).put(Body=json.dumps(disabled_ads, indent=3))
	# Update the formalized cache as well
	formalized_cache = cache_read(event["observer_uuid"], cache_name="formalized_cache")
	_, rdo_uuid = event["rdo_uuid_unsplit"].split(".")
	if (rdo_uuid in formalized_cache):
		formalized_cache[rdo_uuid]["disabled_ad"] = int(time.time())
	cache_write(event["observer_uuid"], cache=formalized_cache, cache_name="formalized_cache")
	return {
			"status" : "SUCCESS",
			"detail" : "DISABLED",
			"observer_uuid" : event["observer_uuid"],
			"rdo_uuid_unsplit" : event["rdo_uuid_unsplit"]
		} # Note: Doesn't actually need to return anything, just doing this for verbatim

def get_disabled_ads(this_observer_uuid):
	disabled_ads_path = f'{this_observer_uuid}/disabled_ads.json'
	disabled_ads = list()
	if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path)):
		disabled_ads = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path).get()['Body'].read())
	return disabled_ads

def process_enable_ad(event):
	disabled_ads_path = f'{event["observer_uuid"]}/disabled_ads.json'
	disabled_ads = get_disabled_ads(event["observer_uuid"])
	disabled_ads = [x for x in disabled_ads if (x != event["rdo_uuid_unsplit"])]
	disabled_ads = list(set(disabled_ads))
	AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path).put(Body=json.dumps(disabled_ads, indent=3))
	# Update the formalized cache as well
	formalized_cache = cache_read(event["observer_uuid"], cache_name="formalized_cache")
	_, rdo_uuid = event["rdo_uuid_unsplit"].split(".")
	if (rdo_uuid in formalized_cache):
		formalized_cache[rdo_uuid]["enabled_ad"] = int(time.time())
	cache_write(event["observer_uuid"], cache=formalized_cache, cache_name="formalized_cache")
	return {
			"status" : "SUCCESS",
			"detail" : "ENABLED",
			"observer_uuid" : event["observer_uuid"],
			"rdo_uuid_unsplit" : event["rdo_uuid_unsplit"]
		} # Note: Doesn't actually need to return anything, just doing this for verbatim


MAX_N_PER_PAGINATION = 10
def process_get_ads_for_observer(event):
	this_observer_uuid = event["observer_uuid"]
	this_offset = int(event["offset"])
	ads = subbucket_contents({"Bucket": S3_BUCKET_MOBILE_OBSERVATIONS, "Prefix":f"{this_observer_uuid}/rdo/"})
	ads_by_timestamp = [{"path" : x, "timestamp" : int(x.split("/")[2].split(".")[0])} for x in ads]

	# TODO - for inhibitions, we need to include filtering at this step <--
	disabled_ads = [f'{this_observer_uuid}/rdo/{x}/' for x in get_disabled_ads(event["observer_uuid"])]
	print(disabled_ads)
	ads_by_timestamp = [x for x in ads_by_timestamp if (not x["path"] in disabled_ads)]
	#print(ads_by_timestamp)

	n_total_ads = len(ads_by_timestamp)


	# Sort by largest timestamps first
	ads_by_timestamp = sorted(ads_by_timestamp, key=lambda d: d["timestamp"], reverse=True)
	ads = [x["path"] for x in ads_by_timestamp[MAX_N_PER_PAGINATION*this_offset:MAX_N_PER_PAGINATION*(this_offset+1)]]
	ad_objs = list()
	for x in ads:
		try:
			this_ad_obj = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{x}output.json").get()['Body'].read())
			ad_objs.append({
					"rdo_uuid_unsplit" : x.split("/")[2],
					"banner_img" : presigned_url(this_ad_obj["media"][0]),
					"observed_at" : this_ad_obj["observation"]["observed_on_device_at"],
					"platform" : this_ad_obj["observation"]["platform"],
					"format" : this_ad_obj["observation"]["ad_format"]
				})
		except:
			print(traceback.format_exc())

	
	return {
		"paginate" : {
			"forward" : (len(ads_by_timestamp[MAX_N_PER_PAGINATION*(this_offset+0+1):MAX_N_PER_PAGINATION*(this_offset+1+1)]) > 0),
			"backward" : (this_offset != 0)
		},
		"ad_objs" : ad_objs,
		"n_total_ads" : n_total_ads,
		"pagination_offset" : this_offset,
		"n_in_pagination" : len(ad_objs)
	}

def get_list_objects_v2_passthrough(Bucket, Prefix):
	output = list()
	tentative_output = get_list_objects_v2(Bucket=Bucket, Prefix=Prefix)
	if ("Contents" in tentative_output):
		for x in tentative_output["Contents"]:
			output.append(x["Key"])
	return output


def process_delete_ad(request_body, dryrun=(__name__=="__main__")):
	observer_uuid = request_body["observer_uuid"]
	rdo_uuid_unsplit = request_body["rdo_uuid_unsplit"]
	_, formalized_uuid = rdo_uuid_unsplit.split(".")
	to_delete = list()

	# RDO
	rdo_entries = get_list_objects_v2_passthrough(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{observer_uuid}/rdo/{rdo_uuid_unsplit}")
	to_delete.extend(rdo_entries)

	# Formalized
	formalized_record = cache_read(observer_uuid, cache_name=f"formalized/{formalized_uuid}")
	to_delete.append(f"{observer_uuid}/formalized/{formalized_uuid}.json")

	# CLIP Classification
	to_delete.append(f"{observer_uuid}/clip_classifications/{formalized_uuid}.json")

	# Temp-V2
	targeted_data_donation_uuids = [x['data_donation_uuid'] for x in formalized_record]
	for x in targeted_data_donation_uuids:
		temp_v2_entries = get_list_objects_v2_passthrough(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=f"{observer_uuid}/temp-v2/{x}")
		to_delete.extend(temp_v2_entries)


	# Cache edits...

	retained = dict()

	# entrypoint_cache
	entrypoint_cache = cache_read(observer_uuid, cache_name=f"entrypoint_cache")
	retained["entrypoint_cache"] = {k:entrypoint_cache[k] for k in targeted_data_donation_uuids}
	if (not dryrun):
		entrypoint_cache = {k:v for k,v in entrypoint_cache.items() if (not k in targeted_data_donation_uuids)}
		cache_write(observer_uuid, cache=entrypoint_cache, cache_name="entrypoint_cache")

	# formalized_cache
	formalized_cache = cache_read(observer_uuid, cache_name=f"formalized_cache")
	retained["formalized_cache"] = {formalized_uuid : formalized_cache[formalized_uuid]}
	if (not dryrun):
		formalized_cache = {k:v for k,v in formalized_cache.items() if (k != formalized_uuid)}
		cache_write(observer_uuid, cache=formalized_cache, cache_name="formalized_cache")

	# clip_classification/tentative_summary
	tentative_summary = cache_read(observer_uuid, cache_name=f"clip_classification/tentative_summary")
	retained["clip_classification/tentative_summary"] = [x for x in tentative_summary if (x["observation.uuid"] == formalized_uuid)]
	if (not dryrun):
		tentative_summary = [x for x in tentative_summary if (not x["observation.uuid"] == formalized_uuid)]
		cache_write(observer_uuid, cache=tentative_summary, cache_name=f"clip_classification/tentative_summary")

	# disabled_ads IS NOT EDITED

	# quick_access_cache
	quick_access_cache = cache_read(observer_uuid, cache_name=f"quick_access_cache")
	retained["quick_access_cache"] = {
			"observations" : [x for x in quick_access_cache["observations"] if (any([(y in x) for y in targeted_data_donation_uuids]))],
			"ads_passed_rdo_construction" : [x for x in quick_access_cache["ads_passed_rdo_construction"] if (formalized_uuid in x)]
		}
	if (not dryrun):
		quick_access_cache["observations"] = [x for x in quick_access_cache["observations"] if (not any([(y in x) for y in targeted_data_donation_uuids]))]
		quick_access_cache["ads_passed_rdo_construction"] = [x for x in quick_access_cache["ads_passed_rdo_construction"] if (not formalized_uuid in x)]
		cache_write(observer_uuid, cache=quick_access_cache, cache_name=f"quick_access_cache")

	s3_key_movements = list()
	for x in to_delete:
		# The convention is that all entries are preceded by an observer UUID - we inject after this the indicator of the formalized UUID
		# as the unique identifier of the entry to delete
		s3_key_movements.append({
				"from" : {
					"bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, 
					"key" : x
				},
				"to" : {
					"bucket" : S3_BUCKET_MOBILE_OBSERVATIONS_STASIS, 
					"key" : x.replace(f"{observer_uuid}", f"{observer_uuid}/{formalized_uuid}")
				}
			})

	retained["statistics"] = {
		"observer_uuid" : observer_uuid,
		"formalized_uuid" : formalized_uuid,
		"at" : int(time.time())
	}

	# Undertake the copy routine
	for this_entry in s3_key_movements:
		try:
			AWS_CLIENT['s3'].copy_object(
				Bucket=this_entry["to"]["bucket"],
				CopySource={"Bucket": this_entry["from"]["bucket"], "Key": this_entry["from"]["key"]},
				Key=this_entry["to"]["key"]
			)
		except: pass
		try:
			AWS_CLIENT['s3'].delete_object(Bucket=this_entry["from"]["bucket"], Key=this_entry["from"]["key"])
		except: pass
	# Store everything about the data movement
	AWS_RESOURCE["s3"].Object(S3_BUCKET_MOBILE_OBSERVATIONS_STASIS, f'{observer_uuid}/{formalized_uuid}/metadata.json').put(Body=json.dumps(retained, indent=3))
	return str()

processes = {
	"GET_ADS" : process_get_ads_for_observer,
	"DISABLE_AD" : process_disable_ad,
	"ENABLE_AD" : process_enable_ad,
	"DELETE_AD" : process_delete_ad
}

def lambda_handler(event, context):
	request_body = dict()
	response_body = dict()
	print(event)
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
			response_body = processes[request_body["action"]](request_body)

	return {
		'statusCode': 200,
		"headers": {
			"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
			"Pragma": "no-cache",
			"Expires": "0"
		},
		'body': json.dumps(response_body)
	}

'''
	This function accepts an observer UUID as a credential, and then loads up
	the following details:

		* The total number of ads the individual has donated

		* The total list of ads the individual has donated (can be paginated)

			* Each ad has:

				* ...

	front-end application (svelte) talks to api gateway which talks to lambda

	svelte hits api gateway (f06kj1k332)

		DELETE

		/prod/ads/335beb8e-c33d-4cbe-bae3-c0b31423a080/1754796044485.dcc7d07a-13bd-41d5-9e5e-6cd5fc2862ee


'''

'''
	Find out what's disabled system-wide
'''
def oct_2025_routine_sw_disabled():
	# Make the dir to house the disabled ads
	disabled_ads_for_all_observers_path = os.path.join(os.getcwd(), "disabled_ads_for_all_observers")
	os.mkdir(disabled_ads_for_all_observers_path)
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		disabled_ads_path = f'{this_observer_uuid}/disabled_ads.json'
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path)):
			print("\tfound!")
			disabled_ads = json.loads(AWS_RESOURCE['s3'].Object(S3_BUCKET_MOBILE_OBSERVATIONS, disabled_ads_path).get()['Body'].read())
			with open(os.path.join(disabled_ads_for_all_observers_path, this_observer_uuid), "w") as f:
				f.write(json.dumps(disabled_ads,indent=3))
				f.close()

def oct_2025_routine_sw_disabled_pt_2():
	# make the 
	pass

def test():
	import requests
	response = requests.post(
			"https://bxxqvaozhe237ak5ndca2zftz40kvgfm.lambda-url.ap-southeast-2.on.aws/", 
			json={
				"action": "DELETE_AD",
				"observer_uuid": "4ccd5b4b-19da-4a34-a627-c9a534a627cd",
				"rdo_uuid_unsplit" : "1748229356094.851e2cf8-5a82-48ad-805b-88f84754f462"
			})
	ipdb.set_trace()








'''
	This routine examines 
		all disabled ads among all observers as actioned on the app
		all disabled ads among all observers as action on the dashboard

	It then cross-compares them to find out
		what is on the dashboard and not on the app
		what is on the app and not on the dashboard

	1. As an assertion, it should be expected that anything hidden on the app is already hidden on the dashboard
	
	2. For those entries hidden on the dashboard that are not yet hidden on the app, make the necessary changes

'''
import csv

def read_csv_as_dicts(path):
	with open(path, newline='', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		return list(reader)

def cross_deletion():
	list_of_vals = read_csv_as_dicts("hidden-ads.csv")
	hidden_ads = {k:dict() for k in ["on_dashboard", "on_app"]}

	# Dashboard-based
	for x in list_of_vals:
		observer_uuid, unsplit_rdo_uuid = x["observation_id"].split("_")
		if (not observer_uuid in hidden_ads["on_dashboard"]):
			hidden_ads["on_dashboard"][observer_uuid] = list()
		hidden_ads["on_dashboard"][observer_uuid].append(unsplit_rdo_uuid)

	# App-based
	app_based_sample_dir = os.path.join(os.getcwd(), "disabled_ads_for_all_observers")
	for observer_uuid in os.listdir(app_based_sample_dir):
		if (not "DS_Store" in observer_uuid):
			unsplit_rdo_uuids = json.loads(open(os.path.join(app_based_sample_dir, observer_uuid)).read())
			for unsplit_rdo_uuid in unsplit_rdo_uuids:
				if (not observer_uuid in hidden_ads["on_app"]):
					hidden_ads["on_app"][observer_uuid] = list()
				hidden_ads["on_app"][observer_uuid].append(unsplit_rdo_uuid)

	#print(json.dumps(hidden_ads,indent=3))

	comparison = dict()
	for x in hidden_ads:
		for y in hidden_ads:
			if (x != y):
				annotation_not = f'{x}_and_not_{y}'
				annotation = f'{x}_and_{y}'
				
				for observer_uuid in hidden_ads[x]:
					# If not instantiated, instantiate...
					for z in [annotation, annotation_not]:
						if (not z in comparison):
							comparison[z] = dict()
						if (not observer_uuid in comparison[z]):
							comparison[z][observer_uuid] = list()
					
					if (observer_uuid in hidden_ads[y]):
						for z in hidden_ads[x][observer_uuid]:
							if (z in hidden_ads[y][observer_uuid]):
								comparison[annotation][observer_uuid].append(z)
							else:
								comparison[annotation_not][observer_uuid].append(z)
					else:
						for z in hidden_ads[x][observer_uuid]:
							comparison[annotation_not][observer_uuid].append(z)
					'''

					for z in hidden_ads[x][observer_uuid]:
						designated_annotation = (annotation_not if (not observer_uuid in hidden_ads[y]) 
							else (annotation_not if (not z in hidden_ads[y][observer_uuid]) else annotation))
						comparison[designated_annotation][observer_uuid].append(z)
					'''


	comparison_flattened = {k:list() for k in comparison}
	[[[comparison_flattened[k].append(k2+"."+v3) for v3 in v2] for k2,v2 in v.items()] for k,v in comparison.items()]

	print(json.dumps(comparison_flattened,indent=3))

	'''

		on_dashboard_and_not_on_app # ones that were hidden by researchers

		on_app_and_not_on_dashboard # ones that were hidden by users

	'''


if (__name__ == "__main__"):
	process_delete_ad({
			"observer_uuid": "4ccd5b4b-19da-4a34-a627-c9a534a627cd",
			"rdo_uuid_unsplit" : "1748229356094.1149a7fd-cc32-402a-bc83-98e6e8ded9cc"
		})
	##oct_2025_routine_sw_disabled()
	''' TODO
	print(json.dumps(json.loads(lambda_handler({
			"action" : "GET_ADS",
			"observer_uuid" : "4ccd5b4b-19da-4a34-a627-c9a534a627cd",
			"offset" : "0"
		}, None)["body"]), indent=3))
	'''
	'''
	print(json.dumps(json.loads(lambda_handler({
			"action" : "ENABLE_AD",
			"observer_uuid" : "4ccd5b4b-19da-4a34-a627-c9a534a627cd",
			"rdo_uuid_unsplit" : "1748229356094.851e2cf8-5a82-48ad-805b-88f84754f462"
		}, None)["body"]), indent=3))
	'''



'''

Observer:
a9c7d8
ID:
3914110d-6bc7-472b-b0f7-85edda9c7d8c


arn:aws:lambda:ap-southeast-2:519969025508:function:moat_observer_dashboard

{
  "action": "DISABLE_AD",
  "observer_uuid": "4ccd5b4b-19da-4a34-a627-c9a534a627cd",
  "rdo_uuid_unsplit" : "1748229356094.851e2cf8-5a82-48ad-805b-88f84754f462"
}

'''




