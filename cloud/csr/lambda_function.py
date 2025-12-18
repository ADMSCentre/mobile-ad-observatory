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
import random
import traceback
from visualize_logs import *


s3_client = boto3.client('s3', region_name='ap-southeast-2')

s3 = boto3.resource('s3')
S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"

def get_list_objects_v2(Bucket=None, Prefix=None):
	result = list()
	paginator = s3_client.get_paginator('list_objects_v2')
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
			for prefix in s3_client.get_paginator("list_objects_v2").paginate(
				**{**{"Delimiter" : "/"}, **kwargs}).search("CommonPrefixes")] if (x is not None)]
	else:
		try:
			for batch_obj in [x for x in s3_client.get_paginator("list_objects_v2").paginate(**{**{"Delimiter" : "/"}, **kwargs})]:
				for key_obj in batch_obj["Contents"]:
					results.append(key_obj["Key"])
		except:
			print(traceback.format_exc())
			# Bucket is probably empty
	return results


'''
	This function visualizes the most recent activity for a given user and is executed locally

	e.g. python lambda_function.py draw_logs "c1a56f0c-8775-4b5e-bc7e-8b9f41039cd5"
'''
TYPES_DEVICE_ORIENTATION = {"P" : "portrait", "L" : "landscape", "U" : "unknown"}
TYPES_BACKGROUND_PROCESSING = {
						"R" : "data_saver_and_restricted", 
						"W" : "data_saver_and_whitelisted", 
						"F" : "unrestricted",
						"U" : "unknown"
					}
TYPES_EVENTS = {
			"REC" : "recording",
			"APP" : "moat",
			"FBK" : "facebook",
			"FBL" : "facebook-lite",
			"TOK" : "tiktok",
			"IGM" : "instagram",
			"YTB" : "youtube",
			"MNL" : "ad_dispatch_manual",
			"BKG" : "ad_dispatch_auto"
		}

TARGET_EVENTS = ["FBK", "FBL", "TOK", "IGM", "YTB"]

def format_duration(seconds: int) -> str:
	hours = seconds // 3600
	minutes = (seconds % 3600) // 60
	secs = seconds % 60

	parts = []
	if hours:
		parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
	if minutes:
		parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
	if secs or not parts:
		parts.append(f"{secs} second{'s' if secs != 1 else ''}")

	return ', '.join(parts)

def s3_object_exists(this_bucket, this_path):
	try:
		s3_client.head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False

NOW = int(time.time())
A_DAY_AGO = NOW - (24 * 60 * 60)
THREE_DAYS_AGO = NOW - (24 * 60 * 60 * 3)
def diagnostic_usage(this_observer_uuid, results,LOOKBEHIND=A_DAY_AGO):
	diagnostics_last_usage = dict()
	#
	to_get = {
		"events" : ["recording","facebook","facebook-lite","tiktok","instagram","youtube"],
		"statistics" : ["accessibility_services_enabled", "battery_optimization_unrestricted", "background_processing_status"]
	}

	desired_values = {
		"accessibility_services_enabled" : True,
		"battery_optimization_unrestricted" : True,
		"background_processing_status" : "unrestricted"
	}
	#
	for y in to_get:
		if (y in results):
			for k in to_get[y]:
				timestamps = list()
				if (y == "events"):
					[timestamps.extend(list(x.values())) for x in results[y][k]]
				if (y == "statistics"):
					timestamps = [int(z) for z in list(results["statistics"][k].keys()) if (results["statistics"][k][z] == desired_values[k])]
				diagnostics_last_usage[k] = None if (len(timestamps) == 0) else max(timestamps)
	diagnostics_amount_usage = dict()
	for k in to_get["events"]:
		usage = None
		partially_captured = False
		if ("events" in results) and (k in results["events"]):
			relevant_entries = [x for x in results["events"][k] 
				if any([((x.get(y,int()) >= LOOKBEHIND) and (x.get(y,int()) <= NOW)) for y in ["start", "end"]])]
			relevant_keys = list()
			[relevant_keys.extend(list(x.keys())) for x in relevant_entries]
			if (set(relevant_keys) == set(["start", "end"])):
				usage = int()
				for x in relevant_entries:
					if (("start" in x) and ("end" in x)):
						usage += abs(x["start"] - x["end"])
				partially_captured = False
			else:
				partially_captured = True
		diagnostics_amount_usage[k] = {
				"usage" : usage,
				"partially_captured" : partially_captured
			}
	diagnostics_amount_usage["aggregate"] = {"usage" : None, "partially_captured" : False}
	for z in ["facebook","facebook-lite","tiktok","instagram","youtube"]:
		if (z in diagnostics_amount_usage):
			if (diagnostics_amount_usage[z]["usage"] is not None):
				if (diagnostics_amount_usage["aggregate"]["usage"] is None):
					diagnostics_amount_usage["aggregate"]["usage"] = diagnostics_amount_usage[z]["usage"]
				else:
					diagnostics_amount_usage["aggregate"]["usage"] += diagnostics_amount_usage[z]["usage"]
				if (not diagnostics_amount_usage["aggregate"]["partially_captured"]):
					diagnostics_amount_usage["aggregate"]["partially_captured"] = diagnostics_amount_usage[z]["partially_captured"]
	return {
			"diagnostics_amount_usage" : diagnostics_amount_usage,
			"diagnostics_last_usage" : diagnostics_last_usage
		}

N_MINUTE_SECONDS = 60
N_HOUR_MINUTES = 60
N_DAY_HOURS = 24
N_WEEK_DAYS = 7
# The max amount of time to look backwards
MAX_LOOKBEHIND = (N_MINUTE_SECONDS * N_HOUR_MINUTES * N_DAY_HOURS * N_WEEK_DAYS)

def routine_csr_generate(event, context, request_body, response_body, consumed=False):
	this_observer_uuid = event["observer_uuid"]
	log_references = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=this_observer_uuid+"/logs")
	if (("Contents" in log_references) and (len(log_references["Contents"]) > 0)):
		joined_at_html = "<!-- 'USER JOIN' DETAILS ARE NOT AVAILABLE - PLEASE REFER TO SYSTEM ADMIN -->"
		joined_at_details = None
		try: joined_at_details = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/joined_at.json').get()['Body'].read())
		except: return response_body
		if (("system_information" in joined_at_details)):
			joined_at_details["system_information"]["joined_at"] = unix_to_brisbane(int(joined_at_details["joined_at_raw"]))
			joined_at_html = dict_to_html_table(joined_at_details["system_information"])
		data_donations = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, "Prefix" : f"{this_observer_uuid}/temp-v2/"})
		data_donations_ts = list()
		for x in data_donations:
			try:
				metadata = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{x}metadata.json').get()['Body'].read())
				observed_at = int(metadata["nameValuePairs"]["observedAt"])
				data_donations_ts.append(observed_at)
			except: pass
		rdos = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, "Prefix" : f"{this_observer_uuid}/rdo/"})
		rdos_ts = [int(int(x.split("/")[-2].split(".")[0]) / 1000) for x in rdos]
		#with open("data_donations_ts.json", "w") as f: f.write(json.dumps(data_donations_ts,indent=3))
		#with open("rdos_ts.json", "w") as f: f.write(json.dumps(rdos_ts,indent=3))
		qualified_log_groups = list()
		current_time = int(time.time())
		#log_references = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=this_observer_uuid+"/logs")
		#if (("Contents" in log_references) and (len(log_references["Contents"]) > 0)):
		for x in log_references["Contents"]:
			this_key = x["Key"]
			dispatch_time = int(this_key.split("/")[-1].replace(".json",str()))
			if (dispatch_time > (current_time-MAX_LOOKBEHIND)):
				qualified_log_groups.append({
						"dispatch_time" : dispatch_time,
						"key" : this_key
					})
		# Load in each log group
		qualified_logs = list()
		for x in qualified_log_groups:
			#print(f"Retrieving log : {x['key']}")
			for y in s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, x["key"]).get()['Body'].read().decode("utf-8").split("\n"):
				if ((len(y) > 0) and (len(y.split(",")) == 10)):
					y_uuid, y_time, y_case, y_orientation, y_accessibility, y_battery, y_bkg_status, y_n_analyses, y_n_videos, y_n_dispatches = y.split(",")
					qualified_logs.append({
							"uuid" : y_uuid,
							"at" : int(y_time),
							"case" : y_case,
							"orientation" : TYPES_DEVICE_ORIENTATION[y_orientation],
							"accessibility_services_enabled" : (y_accessibility == "T"),
							"battery_optimization_unrestricted" : (y_battery == "T"),
							"background_processing_status" : TYPES_BACKGROUND_PROCESSING[y_bkg_status],
							"n_analyses" : int(y_n_analyses),
							"n_screen_recordings" : int(y_n_videos),
							"n_dispatches" : int(y_n_dispatches)
						})
		qualified_logs = sorted(qualified_logs, key=lambda d: d["at"])

		events = {x:list() for x in ["moat", "facebook", "facebook-lite", "tiktok", "instagram", 
										"youtube", "recording", "ad_dispatch_manual", "ad_dispatch_auto"]}

		statistics = {x:dict() for x in [
									"orientation",
									"accessibility_services_enabled",
									"battery_optimization_unrestricted",
									"background_processing_status",
									"n_analyses",
									"n_screen_recordings",
									"n_dispatches"]}

		held_event_alias = None
		for x in qualified_logs:
			event_code = x["case"][:3]
			if ((event_code in TYPES_EVENTS) or (event_code == "TGT")):
				if (event_code == "TGT"):
					event_alias = held_event_alias
				else:
					event_alias = TYPES_EVENTS[event_code]
					if (event_code in TARGET_EVENTS):
						held_event_alias = event_alias
				#
				if (event_alias is not None):
					if (x["case"].endswith("-BGN")):
						events[event_alias].append({ "start" : x["at"] })
					elif (any([x["case"].endswith(y) for y in ["END", "KLL"]])):
						if (len(events[event_alias]) > 0):
							events[event_alias][-1]["end"] = x["at"]
						else:
							events[event_alias].append({"end" : x["at"]})
					for k in statistics:
						statistics[k][str(x["at"])] = x[k]
		#
		if (consumed):
			return {"events" : events, "statistics" : statistics}
		else:
			html_of_report = generate_comprehensive_report_html(this_observer_uuid, joined_at_html, events, statistics, rdos_ts, data_donations_ts)
			if (__name__ == "__main__"):
				with open(f"csr-{this_observer_uuid}.html", "w") as f:
					f.write(html_of_report)
					f.close()
			else:
				s3_client.put_object(
						Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, 
						Key=f'{this_observer_uuid}/csr/{int(time.time())}.html',
						Body=html_of_report, 
						ContentType='text/html'
					)
		#with open("events.json", "w") as f: f.write(json.dumps(events,indent=3))
	return response_body


CACHED_OBSERVER_UUIDS = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})

def get_observer_uuid_from_activation_code(this_activation_code):
	try: return [x for x in CACHED_OBSERVER_UUIDS if (x.replace("/",str())[-7:-1].lower() == this_activation_code.lower())][0].split("/")[0]
	except: 
		print("* Activation code does not exist")
		return None

def get_activation_code_from_observer_uuid(this_observer_uuid):
	return this_observer_uuid[-7:-1].lower()

'''
	Batch event
'''
N_SECONDS_TIMEOUT = (60 * 15)
N_MAX_INVOCATIONS = 200
N_COOLDOWN_WINDOW_SECONDS = (60 * 60 * 1)
def routine_csr_batch(event, context=None, request_body=None, response_body=None):
	lambda_client = boto3.client("lambda", config=botocore.config.Config(
													retries={'max_attempts': 0}, read_timeout=840, 
													connect_timeout=600, region_name="ap-southeast-2"))
	time_at_init = int(time.time())
	observer_uuids = CACHED_OBSERVER_UUIDS
	random.shuffle(observer_uuids)
	n_invocations = int()
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())

		# Do no further processing if the observer's last observation was greater than the MAX_LOOKBEHIND
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/entrypoint_cache.json')):
			entrypoint_cache = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
										f'{this_observer_uuid}/entrypoint_cache.json').get()['Body'].read())
			entrypoint_cache_observed_ats = [(int() if (not "observed_at" in x) else x["observed_at"]) for x in entrypoint_cache.values()]
			if ((len(entrypoint_cache_observed_ats) > 0) and (abs(max(entrypoint_cache_observed_ats) - int(time.time())) < MAX_LOOKBEHIND)):
				# Exit immediately if we've reached the max n invocations, or the timeout
				if ((abs(time_at_init-int(time.time())) > N_SECONDS_TIMEOUT) or (n_invocations > N_MAX_INVOCATIONS)):
					break

				# If the last CSR report was within the cooldown window, pass to the next index
				logs_n = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=this_observer_uuid+"/logs")
				logs_n = 0 if (not "Contents" in logs_n) else len(logs_n["Contents"])
				no_logs_for_csr = (logs_n == 0)
				csr_last_execution = int()
				if (not no_logs_for_csr):
					try: 
						csr_last_execution = max([int(x["Key"].split("/")[-1].replace(".html",str())) for x in 
							get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=this_observer_uuid+"/csr")["Contents"] if (x["Key"].endswith(".html"))])
					except: pass#print(traceback.format_exc())
				execution_was_too_recent = (abs(int(time.time()) - csr_last_execution) < N_COOLDOWN_WINDOW_SECONDS)
				if (not ((no_logs_for_csr) or (execution_was_too_recent))):
					invoke_response = lambda_client.invoke(
							FunctionName="arn:aws:lambda:ap-southeast-2:519969025508:function:moat_csr", 
							InvocationType="Event", 
							Payload=json.dumps({
								"action" : "routine_csr_generate",
								"observer_uuid": this_observer_uuid
							}))
					n_invocations += 1
					time.sleep(5)
	return response_body

def routine_csr_get(event, context=None, request_body=None, response_body=None):
	expiration = 3600
	this_observer_uuid = event["observer_uuid"]
	csr_reports = get_list_objects_v2(Bucket=S3_BUCKET_MOBILE_OBSERVATIONS, Prefix=this_observer_uuid+"/csr")
	this_csr_report = None
	response_body["url"] = None
	if (("Contents" in csr_reports) and (len(csr_reports["Contents"]) > 0)):
		csr_reports = sorted([x["Key"] for x in csr_reports["Contents"]], reverse=True)
		this_csr_report = csr_reports[0]
		if (this_csr_report is not None):
			response_body["url"] = s3_client.generate_presigned_url(
				ClientMethod='get_object',
				Params={
					'Bucket': S3_BUCKET_MOBILE_OBSERVATIONS,
					'Key': this_csr_report
				},
				ExpiresIn=expiration
			)
	return response_body

processes = {
	"routine_csr_batch" : routine_csr_batch,
	"routine_csr_generate" : routine_csr_generate,
	"routine_csr_get" : routine_csr_get
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
			print(traceback.format_exc())

	# Evaluate the action
	if ("action" in request_body):
		if (request_body["action"] in processes):
			print("Action: ", request_body["action"])
			response_body = processes[request_body["action"]](event, context, request_body, response_body)

	return {
		'statusCode': 200,
		'body': json.dumps(response_body)
	}

def safeget(x,y):
	output = x
	try:
		for z in y:
			output = output[z]
	except: return None
	return output

def format_last_config_v(diagnostics, x, duration=False):
	y = safeget(diagnostics, x)
	if (y is None):
		return "N/A" 
	else:
		if (duration):
			return format_duration(y)
		else:
			return (y > int(time.time() - (3*60*60)))#unix_to_brisbane(y, adjust=False)

def diagnostic_usage_report(activation_codes_to_target=["C0C877", "A4989A"]):
	aggregated_results = list()
	for this_activation_code in activation_codes_to_target:
		print(this_activation_code)
		this_observer_uuid = get_observer_uuid_from_activation_code(this_activation_code)
		if (this_observer_uuid is not None):
			rdos = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS, "Prefix" : f"{this_observer_uuid}/rdo/"})
			rdos_ts = [int(int(x.split("/")[-2].split(".")[0]) / 1000) for x in rdos]
			results = routine_csr_generate({"observer_uuid" : this_observer_uuid}, None, dict(), dict(), consumed=True)
			diagnostics = diagnostic_usage(this_observer_uuid, results, LOOKBEHIND=A_DAY_AGO)
			diagnostics_three_days_ago = diagnostic_usage(this_observer_uuid, results, LOOKBEHIND=THREE_DAYS_AGO)
			last_recording_logged_at = safeget(diagnostics, ["diagnostics_last_usage", "recording"])
			last_recording_logged_at = "N/A" if (last_recording_logged_at is None) else unix_to_brisbane(last_recording_logged_at, adjust=False)
			#
			last_accessibility_services_enabled = format_last_config_v(diagnostics, 
					["diagnostics_last_usage","accessibility_services_enabled"])
			last_battery_optimization_unrestricted = format_last_config_v(diagnostics, 
					["diagnostics_last_usage","battery_optimization_unrestricted"])
			last_background_processing_status = format_last_config_v(diagnostics, 
					["diagnostics_last_usage","background_processing_status"])
			
			n_recording_seconds_last_day = format_last_config_v(diagnostics, 
					["diagnostics_amount_usage", "recording", "usage"], duration=True)
			n_recording_seconds_last_3days = format_last_config_v(diagnostics_three_days_ago, 
					["diagnostics_amount_usage", "recording", "usage"], duration=True)
			
			n_social_media_seconds_last_day = format_last_config_v(diagnostics, 
					["diagnostics_amount_usage", "aggregate", "usage"], duration=True)
			n_social_media_seconds_last_3days = format_last_config_v(diagnostics_three_days_ago, 
					["diagnostics_amount_usage", "aggregate", "usage"], duration=True)

			n_rdos_last_2days = len([x for x in rdos_ts if (x > (A_DAY_AGO - (24 * 60 * 60)))])
			n_rdos_all_time = len([x for x in rdos_ts])

			aggregated_results.append({
					"activation_code" : this_activation_code,

					"last_accessibility_services_enabled" : last_accessibility_services_enabled,
					"last_battery_optimization_unrestricted" : last_battery_optimization_unrestricted,
					"last_background_processing_status" : last_background_processing_status,

					"n_recording_seconds_last_day" : n_recording_seconds_last_day,
					"n_recording_seconds_last_3days" : n_recording_seconds_last_3days,

					"n_social_media_seconds_last_day" : n_social_media_seconds_last_day,
					"n_social_media_seconds_last_3days" : n_social_media_seconds_last_3days,

					"n_rdos_last_2days" : n_rdos_last_2days,
					"n_rdos_all_time" : n_rdos_all_time


				})
	import pandas as pd
	df = pd.DataFrame(aggregated_results)
	column_mappings = {
			"activation_code" : "Activation Code",

			"last_accessibility_services_enabled" : "Accessibility Services - Last 3 Hrs",
			"last_battery_optimization_unrestricted" : "Battery Optimization - Last 3 Hrs",
			"last_background_processing_status" : "Background Processing - Last 3 Hrs",


			"n_recording_seconds_last_day" : "Screen-Recording Usage - Last 24 Hours",
			"n_recording_seconds_last_3days" : "Screen-Recording Usage - Last 3 Days",


			"n_social_media_seconds_last_day" : "Social Media Usage - Last 24 Hours",
			"n_social_media_seconds_last_3days" : "Social Media Usage - Last 3 Days",

			"n_rdos_last_2days" : "Ads Collected - Last 48 Hours",
			"n_rdos_all_time" : "Ads Collected - All-Time"

		}
	if column_mappings: df = df.rename(columns=column_mappings)
	df.to_excel("activity_report.xlsx", index=False, engine='openpyxl')
	ipdb.set_trace()

if (__name__ == "__main__"):
	#routine_csr_batch(None)
	#ipdb.set_trace()
	#print(routine_csr_generate({"observer_uuid" : "OBSERVER_UUID_GOES_HERE"}, None, dict(), dict()))
	#print(json.dumps(diagnostic_usage("OBSERVER_UUID_GOES_HERE"), indent=3))
	#diagnostic_usage_report(["ACTIVATION_CODE_1", "ACTIVATION_CODE_2"])
	ipdb.set_trace()
	#









