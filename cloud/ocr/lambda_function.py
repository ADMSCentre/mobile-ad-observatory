'''

	moat_ocr : evaluate raw tentative ads and determine if they are ads or not based on various criteria [TODO]

		SYNOPSIS:

			needs to do a preliminary OCR on sponsored text within ads - this will only evaluate the best candidate of all the images within the data dontation
			that contains the sponsored text (where best candidate is defined as the frame with the most content) and retain the result as a 
			'first-pass' of the ocr - the result will examine whether the sponsored text matches that of the result identified by the in-app classifier
			if there is a match - the result will advance to the grouper

		UPDATE:
		
			NOTE: WE HAVE SET THE REQUIREMENT THAT EVERY SINGLE IMAGE THAT WE RETRIEVE MUST HAVE THE SPONSORED TEXT APPEAR WITHIN IT

			originally devised as a shallow-pass OCR, now we have to make it do all frames in order to qualify them for the ad

'''

import sys
import os
working_dir = "/mnt/fs"
if (len(sys.argv) > 1):
	working_dir = os.getcwd()
sys.path.insert(0,working_dir+"/")
if (__name__ == "__main__"):
	import ipdb
import cv2
import random
import easyocr
import numpy as np
import json
from io import BytesIO
import time
import json
import boto3
import botocore
import traceback
import base64

EASYOCR_READER = easyocr.Reader(["en"])

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

'''
	This function handles the loading of file resources into memory
'''
def load_image_resource(key):
	file_stream = BytesIO()
	print(key)
	this_img_raw = s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, key).download_fileobj(file_stream)
	np_1d_array = np.frombuffer(file_stream.getbuffer(), dtype="uint8")
	return cv2.imdecode(np_1d_array, cv2.IMREAD_COLOR)

'''
	This function extracts visual texts from a supplied image, using
	OCR techniques to do so. It then organises the data into a response.
'''
def img_ocr_data(EASYOCR_READER, img, s=1):
	return [{
			"x": round(int(x[0][0][0])/s), 
			"y": round(int(x[0][0][1])/s),
			"w": round(abs(int(x[0][0][0]) - int(x[0][1][0]))/s), 
			"h": round(abs(int(x[0][0][1]) - int(x[0][2][1]))/s), 
			"text": x[1], "confidence": float(x[2])
		} for x in EASYOCR_READER.readtext(img)]

def s3_object_exists(this_bucket, this_path):
	try:
		s3_client.head_object(Bucket=this_bucket, Key=this_path)
		return True
	except:
		return False
	return False

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

N_SECONDS_TIMEOUT = (60 * 14)

def routine_instance(event, context):
	try:
		# For a given data donation UUID and observer UUID
		data_donation_uuid = event["this_data_donation_uuid"]
		this_observer_uuid = event["this_observer_uuid"]
		#print("Running OCR on data_donation_uuid:", data_donation_uuid, " this_observer_uuid:", this_observer_uuid)
		# Get the metadata for the data donation
		data_donation_metadata = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
			f"{this_observer_uuid}/temp-v2/{data_donation_uuid}/metadata.json").get()['Body'].read())
		# Run OCR on all frames
		# NOTE: We are making an assumption here that we never have more images than can be processed in a single
		# instance of the code - when we reach the timeout (which happens after 15 mins for Lambda), it'll drop
		# off, but to avoid data loss and recover, we can get it to evaluate whether OCRs exist beforehand
		frame_numbers = [x for x in data_donation_metadata["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"]]
		for this_frame in frame_numbers:
			elapsed_time = abs(int(time.time()) - event["time_at_init"])
			if ((elapsed_time > N_SECONDS_TIMEOUT) and (__name__ != "__main__")): 
				print("Calling early exit on timeout (within instance)...")
				break
			#print("Analyzing frame:", this_frame)
			this_frame_path = f'{this_observer_uuid}/temp-v2/{data_donation_uuid}/{this_frame}.jpg'
			this_frame_path_ocr = f'{this_observer_uuid}/temp-v2/{data_donation_uuid}/{this_frame}.jpg.ocr.json'
			if ((s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, this_frame_path)) 
					and (not s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, this_frame_path_ocr))):
				s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS,this_frame_path_ocr).put(
					Body=json.dumps(img_ocr_data(EASYOCR_READER, load_image_resource(this_frame_path)), indent=3))
				if (__name__ == "__main__"):
					print("\t\tApplied!")
			else:
				if (__name__ == "__main__"):
					print("\t\tFrame already exists!")

		# If the code block reaches this point, the process is done, and the data point can be applied
		#if ((__name__ != "__main__") or ((actual_evaluations_to_do == 0) and (__name__ == "__main__"))):
		entrypoint_cache = cache_read(this_observer_uuid, "entrypoint_cache", dict())
		entrypoint_cache[data_donation_uuid]["ocr"] = True
		cache_write(this_observer_uuid, entrypoint_cache, "entrypoint_cache")
		if (__name__ == "__main__"):
			print(f"Executed for data donation {data_donation_uuid}")
	except botocore.exceptions.ClientError as e:
		print(traceback.format_exc())
		#raise Exception()
		time.sleep(10)
	except:
		print(traceback.format_exc())
		if (__name__ == "__main__"):
			ipdb.set_trace()



'''
	Batch event
'''
def routine_batch(event, context=None):
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	data_donations_for_ocr_processing = list()
	# For each observer, if the entrypoint_cache exists, load in the data donations that need OCR
	for this_observer_uuid in observer_uuids:
		print(this_observer_uuid)
		entrypoint_cache_path = f"{this_observer_uuid}entrypoint_cache.json"
		#print(entrypoint_cache_path)
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)):
			print("\t Entrypoint cache exists!")
			entrypoint_cache = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path).get()['Body'].read())
			for k in entrypoint_cache:
				if ((not (("ocr" in entrypoint_cache[k]) and entrypoint_cache[k]["ocr"])) and ("observed_at" in entrypoint_cache[k])):
					data_donations_for_ocr_processing.append({
						"this_data_donation_uuid" : k,
						"observed_at" : entrypoint_cache[k]["observed_at"],
						"this_observer_uuid" : this_observer_uuid.replace("/",str())
					})
	# Shuffle the ads for randomness - UPDATE: do longitudinally instead, to minimize duplication issues at formalization - by randomizing, we jumble the grouping process
	# - here we overcome this
	#random.shuffle(data_donations_for_ocr_processing)
	data_donations_for_ocr_processing = sorted(data_donations_for_ocr_processing, key=lambda d: d["observed_at"])
	random.shuffle(data_donations_for_ocr_processing)

	print(f"Identified {len(data_donations_for_ocr_processing)} data donations for processing...")
	if (__name__ == "__main__"):
		ipdb.set_trace()
	s3.Object("fta-mobile-observations-holding-bucket", 
		f'data_donations_for_ocr_processing/{int(time.time())}.json').put(Body=json.dumps({"data_donations_for_ocr_processing" : len(data_donations_for_ocr_processing)}, indent=3))

	# Take n and process (we attempt to process as many as we can - if the execution drops off, we can always pick
	# it up later, although separating the execution into separate sub-instance style lambdas would be more computationally
	# expensive, as we would need to reload the easyocr module each time - this is why we try to do it all in one hit)
	for entry in data_donations_for_ocr_processing:
		elapsed_time = abs(int(time.time()) - time_at_init)
		if ((elapsed_time > N_SECONDS_TIMEOUT) and (__name__ != "__main__")): 
			print("Calling early exit on timeout...")
			break
		routine_instance(entry | {"time_at_init" : time_at_init}, None)
	return str()


processes = { "batch" : routine_batch }

def lambda_handler(event, context):
	print(os.listdir(os.getcwd()))
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


if (__name__ == "__main__"):
	'''
	routine_instance({
			"this_observer_uuid" : "87ab3985-4f6e-4f6e-9eff-7a1b1f58cfca",
			"this_data_donation_uuid" : "0279d696-88c1-4189-add9-7d416c59798b"
		}, None)
	'''
	routine_batch(dict())







