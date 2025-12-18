'''
	process needs to wake up - for all content that does not yet have failsafe evaluation on it, but does have ocr

	go over the content and evaluate it...

		if the OCR does not contain the sponsored term

			run the necessary yolo model on it <- within an image augmenter that antiicpates the cropping from which the ad comes

			store the result (may be a positive match or not - so long as we've run the check)

		else

			do nothing

		finish up by adjusting the entrypoint cache to signal that the yolo
'''


import sys
import os

working_dir = "/mnt/fs"
if (__name__ == "__main__"):
	working_dir = os.getcwd()
sys.path.insert(0,working_dir+"/")
models_dir = os.path.join("/", "mnt", "fs")
if (__name__ == "__main__"):
	import ipdb
	models_dir = os.getcwd()
import random
import json
from io import BytesIO
import time
import math
import json
import boto3
import botocore
import cv2
import numpy as np
import traceback
import base64
from ultralytics import YOLO

YOLOV5_MODELS = dict()
for x in ["FACEBOOK", "INSTAGRAM", "TIKTOK", "YOUTUBE"]:
	YOLOV5_MODELS[x] = YOLO(os.path.join(models_dir, "models", f"float32_{x.lower()}_sponsored.pt"))

def yolov5_prediction(model, this_img):
	result = model.predict(source=cv2.resize(this_img,(640,640)))
	bounding_boxes = list()
	for i in range(len(result[0].boxes.cls.tolist())):
		bounding_boxes.append({
			"className" : result[0].names[int(result[0].boxes.cls[i])],
			"confidence" : float(result[0].boxes.conf[i]),
			"x1" : float(result[0].boxes.xyxyn[i][0]),
			"y1" : float(result[0].boxes.xyxyn[i][1]),
			"x2" : float(result[0].boxes.xyxyn[i][2]),
			"y2" : float(result[0].boxes.xyxyn[i][3]),
			"w" : float(result[0].boxes.xywhn[i][2]),
			"h" : float(result[0].boxes.xywhn[i][3]),
			"cx" : float(result[0].boxes.xywhn[i][0]),
			"cy" : float(result[0].boxes.xywhn[i][1])
		})
	return bounding_boxes

YOLO_INSERTION_PROPORTIONS = {
		"TYPE_FEED" : {
			"x1" : 0.0,
			"x2" : 1.0,
			"y1" : 0.22869,
			"y2" : 0.88357
		},
		"TYPE_STORY" : {
			"x1" : 0.0,
			"x2" : 1.0,
			"y1" : 0.03118,
			"y2" : 0.86070
		},
		"TYPE_THUMBNAIL" : {
			"x1" : 0.0,
			"x2" : 0.49549,
			"y1" : 0.15592,
			"y2" : 0.46777
		},
		"TYPE_REEL_FOOTER" : {
			"x1" : 0.0,
			"x2" : 1.0,
			"y1" : 0.87318,
			"y2" : 0.98752
		},
		"TYPE_REEL" : {
			"x1" : 0.0,
			"x2" : 1.0,
			"y1" : 0.10395,
			"y2" : 0.88357
		},
		"TYPE_PREVIEW_VIDEO_PORTRAIT" : {
			"x1" : 0.0,
			"x2" : 1.0,
			"y1" : 0.05197,
			"y2" : 0.41580
		}
	}

APP_CONTENT_TYPE_MAPPINGS = {
		"FACEBOOK" : {
			"REEL_FOOTER_BASED" : "TYPE_REEL_FOOTER",
			"REEL_BASED" : "TYPE_REEL",
			"MARKETPLACE_BASED" : "TYPE_THUMBNAIL",
			"STORY_BASED" : "TYPE_STORY",
			"FEED_BASED" : "TYPE_FEED"
		},
		"INSTAGRAM" : {
			"REEL_BASED" : "TYPE_REEL",
			"STORY_BASED" : "TYPE_STORY",
			"FEED_BASED" : "TYPE_FEED"
		},
		"TIKTOK" : {
			"THUMBNAIL" : "TYPE_THUMBNAIL",
			"REEL_FROM_SEARCH" : "TYPE_REEL",
			"REEL_FROM_HOME" : "TYPE_REEL"
		},
		"YOUTUBE" : {
			"REEL_BASED" : "TYPE_REEL",
			"PREVIEW_LANDSCAPE_BASED" : None,
			"APP_FEED_BASED" : "TYPE_FEED",
			"PRODUCT_FEED_BASED" : "TYPE_FEED",
			"GENERAL_FEED_BASED" : "TYPE_FEED",
			"PREVIEW_PORTRAIT_BASED" : "TYPE_PREVIEW_VIDEO_PORTRAIT"
		}
	}

CONFIDENCE_THRESHOLD = {
		"FACEBOOK" : 0.05,
		"INSTAGRAM" : 0.40,
		"TIKTOK" : 0.50,
		"YOUTUBE" : 0.50
	}

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

contextualised_sponsorship_terms = {
	"FACEBOOK" : {
		"SPONSORED_TEXT" : ["Sponsored"]
	},
	"TIKTOK" : {
		"SPONSORED_TEXT" : ["Sponsored"],
		"PROMOTIONAL_CONTENT_TEXT" : ["Promotional content"],
		"PAID_PARTNERSHIP_TEXT" : ["Paid partnership"]
	},
	"INSTAGRAM" : {
		"SPONSORED_TEXT" : ["Sponsored"]
	},
	"YOUTUBE" : {
		"SPONSORED_TEXT" : ["Sponsored"],
		"PRODUCT_IN_THIS_VIDEO_TEXT" : ["Product in this video", "Products in this video"],
		"SPONSORED_TEXT_HORIZONTAL" : ["Sponsored"]
	}
}

'''
	This function 
'''
def dynamic_levenshtein_threshold(s1, s2, threshold=0.5):
	min_str_len = min(len(s1), len(s2))
	# While 50% does look like a large measure of difference, we account for the 
	# larger string's trailing characters that also have to be factored in to the difference
	# between the two strings
	return math.ceil((min_str_len *threshold))

def levenshtein(s1, s2):
	if len(s1) > len(s2):
		s1, s2 = s2, s1
	distances = range(len(s1) + 1)
	for i2, c2 in enumerate(s2):
		distances_ = [i2+1]
		for i1, c1 in enumerate(s1):
			if c1 == c2:
				distances_.append(distances[i1])
			else:
				distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
		distances = distances_
	return distances[-1]

def sliding_levenshtein(s1, s2, threshold=0.25):
	larger_string = (s1 if (len(s2) < len(s1)) else s2)
	smaller_string = (s2 if (len(s2) < len(s1)) else s1)
	this_page_name_processed = larger_string.lower()
	this_query_string_processed = smaller_string.lower()
	MIN_QUERY_STRING_LENGTH = 4
	if (len(this_query_string_processed) < MIN_QUERY_STRING_LENGTH):
		return False
	else:
		if (len(this_page_name_processed) < len(this_query_string_processed)):
			return (levenshtein(this_page_name_processed, this_query_string_processed) <= dynamic_levenshtein_threshold(this_query_string_processed, this_query_string_processed, threshold))
		else:
			for i in range(len(this_page_name_processed)-len(this_query_string_processed)+1):
				if (levenshtein(this_page_name_processed[i:i+len(smaller_string)], this_query_string_processed) <= dynamic_levenshtein_threshold(this_query_string_processed, this_query_string_processed, threshold)):
					return True
			return False

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

def img_failsafe_data(this_path, this_platform, this_img):
	model = YOLO(os.path.join(os.getcwd(), "models", f"float32_{this_platform.upper()}_sponsored.pt")) # TODO - move to a separate file
	result = model.predict(source=cv2.resize(this_img,(640,640)))
	bounding_boxes = list()
	for i in range(len(result[0].boxes.cls.tolist())):
		bounding_boxes.append({
			"className" : result[0].names[int(result[0].boxes.cls[i])],
			"confidence" : float(result[0].boxes.conf[i]),
			"x1" : float(result[0].boxes.xyxyn[i][0]),
			"y1" : float(result[0].boxes.xyxyn[i][1]),
			"x2" : float(result[0].boxes.xyxyn[i][2]),
			"y2" : float(result[0].boxes.xyxyn[i][3]),
			"w" : float(result[0].boxes.xywhn[i][2]),
			"h" : float(result[0].boxes.xywhn[i][3]),
			"cx" : float(result[0].boxes.xywhn[i][0]),
			"cy" : float(result[0].boxes.xywhn[i][1])
		})
	return bounding_boxes

def synthesize_in_the_wild_img(image_to_fit, this_platform, this_ad_type):
	canvas = image_to_fit.copy()
	if (APP_CONTENT_TYPE_MAPPINGS[this_platform][this_ad_type] is not None):
		ascribed_dims = (962,444)
		this_proportions = YOLO_INSERTION_PROPORTIONS[APP_CONTENT_TYPE_MAPPINGS[this_platform][this_ad_type]]

		fit_dim_w = int(math.floor((abs(this_proportions["x1"] - this_proportions["x2"]) * ascribed_dims[1]) - 1))
		fit_dim_h = int(math.floor((abs(this_proportions["y1"] - this_proportions["y2"]) * ascribed_dims[0]) - 1))

		canvas = np.zeros([ascribed_dims[0],ascribed_dims[1],3],dtype=np.uint8); canvas.fill(255)
		itf_h, itf_w, _ = image_to_fit.shape
		fit_dim_ratio = itf_h / itf_w

		adjusted_itf_h, adjusted_itf_w = (0,0)
		if (itf_w > itf_h):
			adjusted_itf_w = int(math.floor(fit_dim_w / fit_dim_ratio))
			adjusted_itf_h = fit_dim_h 
		else:
			adjusted_itf_w = fit_dim_w
			adjusted_itf_h = int(math.floor(fit_dim_h * fit_dim_ratio))

		insertion_x1 = int(math.floor(this_proportions["x1"] * ascribed_dims[1]))
		insertion_y1 = int(math.floor(this_proportions["y1"] * ascribed_dims[0]))
		insertion_x2 = min(insertion_x1 + adjusted_itf_w, ascribed_dims[1] - 1)
		insertion_y2 = min(insertion_y1 + adjusted_itf_h, ascribed_dims[0] - 1)
		canvas[insertion_y1:insertion_y2,insertion_x1:insertion_x2] = cv2.resize(image_to_fit,(
																			int(math.floor(abs(insertion_x1-insertion_x2))),
																			int(math.floor(abs(insertion_y1-insertion_y2)))))
	return canvas

def routine_instance(event, context):
	print(event)
	'''
		firstly load in the metadata and all frames

		as well as the ocr content

		for each frame, compare for sponsorship information on the ocrs

		then record all match information
	'''
	try:
		data_donation_uuid = event["this_data_donation_uuid"]
		this_observer_uuid = event["this_observer_uuid"]
		data_donation_metadata = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
			f"{this_observer_uuid}/temp-v2/{data_donation_uuid}/metadata.json").get()['Body'].read())
		ocrs = {k:json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
			f"{this_observer_uuid}/temp-v2/{data_donation_uuid}/{k}.jpg.ocr.json").get()['Body'].read()) for k in event["frames"]}
		a_frame = str(list(event["frames"].keys())[0])
		this_platform = data_donation_metadata["nameValuePairs"]["platform"]
		frame_sponsored_terms = dict()
		for k in event["frames"]:
			frame_sponsored_terms[k] = data_donation_metadata["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][k]["internalJSONObject"]["nameValuePairs"]["inference"]["internalJSONObject"]["nameValuePairs"]["boundingBoxSponsored"]["internalJSONObject"]["nameValuePairs"]["className"]
		ocr_frame_sponsored_evaluations = {k:[any([sliding_levenshtein(x["text"], y) for y in contextualised_sponsorship_terms[this_platform][frame_sponsored_terms[k]]]) for x in ocrs[k]] for k in ocrs}
		'''
			if there is no match thus far, we proceed to the yolov5 sponsorship detectoin
		'''
		frame_ad_types = dict()
		yolov5_detections = dict()
		for k in event["frames"]:
			frame_ad_types[k] = data_donation_metadata["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][k]["internalJSONObject"]["nameValuePairs"]["adType"]
			if (not any(ocr_frame_sponsored_evaluations[k])):
				this_img_path = f"{this_observer_uuid}/temp-v2/{data_donation_uuid}/{k}.jpg"
				this_img_augmented = synthesize_in_the_wild_img(load_image_resource(this_img_path), this_platform, frame_ad_types[k])
				yolov5_detections[k] = yolov5_prediction(YOLOV5_MODELS[this_platform], this_img_augmented)
			else:
				yolov5_detections[k] = None
		'''
			evaluate if there is a yolov5 indicator

			sign off by indicating that hte yolov5 catcher has run its course on the entrypoint cache
		'''
		output = dict()
		for k in event["frames"]:
			output[k] = {
					"ad_type" : frame_ad_types[k],
					"contextualised_sponsorship_terms" : contextualised_sponsorship_terms[this_platform][frame_sponsored_terms[k]],
					"claimed_sponsored_terms" : frame_sponsored_terms[k],
					"ocr_frame_sponsored_evaluations" : ocr_frame_sponsored_evaluations[k],
					"yolov5_detections" : yolov5_detections[k]
				}
		this_failsafe_path = f'{this_observer_uuid}/temp-v2/{data_donation_uuid}/failsafe.json'
		s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS,this_failsafe_path).put(
						Body=json.dumps(output, indent=3))
		entrypoint_cache = cache_read(this_observer_uuid, "entrypoint_cache", dict())
		entrypoint_cache[data_donation_uuid]["failsafe"] = int(time.time())
		cache_write(this_observer_uuid, entrypoint_cache, "entrypoint_cache")
	except:
		print(traceback.format_exc())



'''
	Batch event
'''
N_SECONDS_TIMEOUT = (60 * 10)
def routine_batch(event, context=None):
	this_keyword = "failsafe"
	time_at_init = int(time.time())
	# Get all observer UUIDsv
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	data_donations_for_processing = list()
	# For each observer, if the entrypoint_cache exists, load in the data donations that need OCR
	for this_observer_uuid in observer_uuids:
		print(this_observer_uuid)
		entrypoint_cache_path = f"{this_observer_uuid}entrypoint_cache.json"
		print(entrypoint_cache_path)
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)):
			print("\t Entrypoint cache exists!")
			entrypoint_cache = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path).get()['Body'].read())
			for k in entrypoint_cache:
				if ((("ocr" in entrypoint_cache[k]) and (entrypoint_cache[k]["ocr"]))
						and (not (this_keyword in entrypoint_cache[k]))):
					if ("observed_at" in entrypoint_cache[k]):
						data_donations_for_processing.append({
							"this_data_donation_uuid" : k,
							"observed_at" : entrypoint_cache[k]["observed_at"],
							"this_observer_uuid" : this_observer_uuid.replace("/",str()),
							"frames" : entrypoint_cache[k]["frames"]
						})
	# Shuffle the ads for randomness - UPDATE: do longitudinally instead, to minimize duplication issues at formalization - by randomizing, we jumble the grouping process
	# - here we overcome this
	#random.shuffle(data_donations_for_ocr_processing)
	data_donations_for_processing = sorted(data_donations_for_processing, key=lambda d: d["observed_at"])
	print(f"Identified {len(data_donations_for_processing)} data donations for processing...")

	# Take n and process (we attempt to process as many as we can - if the execution drops off, we can always pick
	# it up later, although separating the execution into separate sub-instance style lambdas would be more computationally
	# expensive, as we would need to reload the easyocr module each time - this is why we try to do it all in one hit)
	for entry in data_donations_for_processing:
		elapsed_time = abs(int(time.time()) - time_at_init)
		if (elapsed_time > N_SECONDS_TIMEOUT): 
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
			"this_observer_uuid" : "OBSERVER_UUID_GOES_HERE",
			"this_data_donation_uuid" : "DATA_DONATION_UUID_GOES_HERE"
		}, None)
	'''
	'''
	routine_instance({
			"this_observer_uuid" : "OBSERVER_UUID_GOES_HERE",
			"this_data_donation_uuid" : "DATA_DONATION_UUID_GOES_HERE",
			"observed_at" : 1751331157,
			"frames": {
		         "180": 1751331160.631124
		      }
		}, None)
	'''
	routine_batch(dict())



