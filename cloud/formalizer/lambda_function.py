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

if (__name__ == "__main__"):
	import ipdb
import sys
import os
import random
import uuid
import json
import time
import json
import boto3
import botocore
import traceback
import base64

from rect_overlaps import *


def create_dir(path):
	try: os.mkdir(path)
	except: pass

def avg(x):
	try:
		return (sum(x)/ len(x))
	except:
		return float()

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



'''
	Batch event
'''
MIN_CHARACTERS_TO_REGARD = 3
MAX_LEVENSHTEIN_DISTANCE = 2
def lev_sim_pct(s1, s2):
	maximum = min(len(s1),len(s2))
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
		if all((x >= maximum for x in distances_)):
			return 0.0
		distances = distances_
	return (1.0 - min(1.0,(distances[-1] / maximum)))


def get_ocrs_on_data_donation_frame(this_observer_uuid, this_data_donation_uuid, this_frame):
	ocr_path = f'{this_observer_uuid}/temp-v2/{this_data_donation_uuid}/{this_frame}.jpg.ocr.json'
	#print(ocr_path)
	this_ocr = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, ocr_path).get()['Body'].read())
	ocr_terms = list()
	[ocr_terms.extend([y for y in x["text"].split(" ") if (len(y) > MIN_CHARACTERS_TO_REGARD)]) for x in this_ocr]
	return ocr_terms

def ocrs_similarity_forward(ocrs_a, ocrs_b):
	try:
		return min(1.0, sum([max([lev_sim_pct(x, y) for y in ocrs_b]) for x in ocrs_a]) / len(ocrs_a))
	except:
		return 0.0

def ocrs_similarity(ocrs_a, ocrs_b):
	return max(ocrs_similarity_forward(ocrs_a, ocrs_b), ocrs_similarity_forward(ocrs_b, ocrs_a))

def create_test_dir(this_observer_uuid, grouped_timewise):
	test_dir = os.path.join(os.getcwd(), f"tests_{int(time.time())}"); create_dir(test_dir)
	test_observer_dir = os.path.join(test_dir, this_observer_uuid); create_dir(test_observer_dir)
	for i in range(len(grouped_timewise)):
		this_group = grouped_timewise[i]
		test_observer_group_dir = os.path.join(test_observer_dir, str(i)); create_dir(test_observer_group_dir)
		for frame_obj in this_group:
			frame_obj_fname = os.path.join(test_observer_group_dir, f"{frame_obj['data_donation_uuid']}_{frame_obj['frame_n']}.jpg")
			print(frame_obj_fname)
			s3_client.download_file(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/temp-v2/{frame_obj['data_donation_uuid']}/{frame_obj['frame_n']}.jpg", frame_obj_fname)


'''
	
	for a given observer

		go to the formalized cache

		we have a blanket assumption that ads haev to include some form of text - and some form of positioning for said text

		we go over the frames and construct polygons for texts

		fit the texts together, and get likeness measures

		we no longer use 5 second brackets - instead we use 5 second relative brackets



'''

def frame_id(this_frame):
	return f'{this_frame["data_donation_uuid"]}_{this_frame["frame_n"]}'

cached_frames = dict()
N_TIME_GAP_WEAK = 60

def are_frames_connected(this_observer_uuid, last_frame, current_frame, data_donation_failsafe_cache, data_donation_metadata_cache):
	N_TIME_GAP_STRONG = 1
	MIN_FRAME_SIMILARITY_PCT = 0.70
	MIN_FRAME_BBOX_SIMILARITY_PCT = 0.75
	are_connected = False
	# Time-wise analysis
	time_difference_seconds = abs(last_frame["timestamp"] - current_frame["timestamp"])
	strongly_close_timewise = (time_difference_seconds <= N_TIME_GAP_STRONG)
		
	###
	### Load in metadata content
	###
	relevant_data_donation_uuids = list(set([x["data_donation_uuid"] for x in [last_frame, current_frame]]))
	metadata_a = data_donation_metadata_cache[last_frame["data_donation_uuid"]]
	metadata_b = data_donation_metadata_cache[current_frame["data_donation_uuid"]]

	frame_similarity_pct = None
	frame_ocr_bbox_similarity_pct = None
	levenshtein_similarities = None
	
	if (strongly_close_timewise):
		frame_similarity_pct = frame_similarities(metadata_a, metadata_b, last_frame["frame_n"], current_frame["frame_n"])
		strong_frame_overlap = (frame_similarity_pct >= MIN_FRAME_SIMILARITY_PCT)
		print("frame_similarity_pct:", frame_similarity_pct)
		if ((strongly_close_timewise) and (strong_frame_overlap)):
			are_connected = True
			relating_reason = "STRONG_TIME_STRONG_FRAME_BBOXES"
			confidence_pct = frame_similarity_pct
	
	if (not are_connected):
		weakly_close_timewise = (time_difference_seconds <= N_TIME_GAP_WEAK)
		relevant_frames = {frame_id(x):x for x in [last_frame, current_frame]}
		# Cull memory from cached entries
		cached_frames_keys = list(cached_frames.keys())
		for k in cached_frames_keys:
			if (not k in relevant_frames):
				del cached_frames[k]
		# Apply new content
		for k in relevant_frames:
			if (not k in cached_frames):
				print(f"{this_observer_uuid}/temp-v2/{relevant_frames[k]['data_donation_uuid']}/{relevant_frames[k]['frame_n']}.jpg")
				cached_frames[k] = {
					"frame_n" : relevant_frames[k]['frame_n'],
					"ocr" : json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
						f"{this_observer_uuid}/temp-v2/{relevant_frames[k]['data_donation_uuid']}/{relevant_frames[k]['frame_n']}.jpg.ocr.json").get()['Body'].read())
				}
		last_frame_fobj = cached_frames[frame_id(last_frame)]
		ocr_a = last_frame_fobj["ocr"]
		frame_n_a = last_frame_fobj["frame_n"]
		current_frame_fobj = cached_frames[frame_id(current_frame)]
		ocr_b = current_frame_fobj["ocr"]
		frame_n_b = current_frame_fobj["frame_n"]
		levenshtein_similarities = frame_ocr_bbox_similarities(last_frame, current_frame, metadata_a, metadata_b, ocr_a, ocr_b, frame_n_a, frame_n_b, data_donation_failsafe_cache)
		if (len(levenshtein_similarities) > 1): # At bare minimum, we are comparing 'sponsored' text
			similarity_pcts = list(); [similarity_pcts.extend([x["similarity_pct"] for y in range(int(math.ceil(avg([len(x["text_a"]), len(x["text_b"])]))))]) 
														for x in levenshtein_similarities if (type(x["similarity_pct"]) is float)]
			frame_ocr_bbox_similarity_pct = avg(similarity_pcts)
			if (frame_ocr_bbox_similarity_pct >= (0.25 if (strongly_close_timewise) else 0.5)):
				are_connected = True
				relating_reason = "WEAK_TIME_STRONG_OCR_BBOXES"
				confidence_pct = frame_ocr_bbox_similarity_pct
		'''
		frame_ocr_bbox_similarity_pct = frame_ocr_bbox_similarities(metadata_a, metadata_b, ocr_a, ocr_b, frame_n_a, frame_n_b)
		print("frame_ocr_bbox_similarity_pct:", frame_ocr_bbox_similarity_pct)
		strong_frame_ocr_bbox_overlap = (frame_ocr_bbox_similarity_pct >= MIN_FRAME_BBOX_SIMILARITY_PCT)
		if ((weakly_close_timewise) and (strong_frame_ocr_bbox_overlap)):
			are_connected = True
			relating_reason = "WEAK_TIME_STRONG_OCR_BBOXES"
			confidence_pct = frame_ocr_bbox_similarity_pct
		'''

	outcome = None
	if (are_connected):
		outcome = {
				"a" : frame_id(last_frame),
				"b" : frame_id(current_frame),
				"outcome" : relating_reason,
				"confidence_pct" : confidence_pct,
				"diagnostic" : {
					"frame_ocr_bbox_similarity_pct" : frame_ocr_bbox_similarity_pct,
					"frame_similarity_pct" : frame_similarity_pct,
					"levenshtein_similarities" : levenshtein_similarities
				}
			}
	else:
		outcome = {
				"a" : frame_id(last_frame),
				"b" : frame_id(current_frame),
				"outcome" : "UNRELATED",
				"diagnostic" : {
					"frame_ocr_bbox_similarity_pct" : frame_ocr_bbox_similarity_pct,
					"frame_similarity_pct" : frame_similarity_pct,
					"levenshtein_similarities" : levenshtein_similarities
				}
			}
	return are_connected, outcome

def routine_instance_v2(this_observer_uuid, N_TO_PROCESS_IN_ONE_INSTANCE=250):
	#
	entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
	entrypoint_cache = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path).get()['Body'].read())
	entrypoint_cache_ordered = [entrypoint_cache[k] | {"k" : k} for k in entrypoint_cache.keys()]
	entrypoint_cache_ordered = [x for x in entrypoint_cache_ordered if ("observed_at" in x)]
	entrypoint_cache_ordered = sorted(entrypoint_cache_ordered, key=lambda d: d["observed_at"])
	#
	# firstly check the entrypoint cache to determine which data donations:
	# 	1. have ocrs
	# 	2. not have formalized v2 on them
	selected_entries = [x["k"] for x in entrypoint_cache_ordered if ((("ocr" in x) and ("failsafe" in x))
														and (not (("formalized_v2" in x) or ("formalized" in x))))]
	# of those that fit the criteria, expand their frames and get an idea of how many routines we will be undertaking
	expanded_data_donations = list()
	for k in selected_entries:
		for frame_n in entrypoint_cache[k]["frames"]:
			expanded_data_donations.append({
					"data_donation_uuid" : k,
					"timestamp" : entrypoint_cache[k]["frames"][frame_n],
					"frame_n" : frame_n
				})
		# set a cap on how many we are going to relate in one sitting
		if ((N_TO_PROCESS_IN_ONE_INSTANCE is not None) and (len(expanded_data_donations) > N_TO_PROCESS_IN_ONE_INSTANCE)):
			print("Breaking loop early : N_TO_PROCESS_IN_ONE_INSTANCE")
			break
	expanded_data_donations = sorted(expanded_data_donations, key=lambda d: d["timestamp"])
	# evaluate each frame to determine whether its an ad or not - this is done in two ways
	#	1. we identify the sponsored text within the ocr
	#	2. we identify the sponsored term within the yolo
	# we can rely on the failsafe compilation to get an indication on how to proceed here
	unique_data_donation_uuids = list(set([x["data_donation_uuid"] for x in expanded_data_donations]))
	#print(f"{this_observer_uuid}/temp-v2/{expanded_data_donations[0]['data_donation_uuid']}/failsafe.json")
	elapsed_time = int(time.time())
	data_donation_failsafe_cache = {x:json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
		f"{this_observer_uuid}/temp-v2/{x}/failsafe.json").get()['Body'].read()) for x in unique_data_donation_uuids}
	data_donation_metadata_cache = {x:json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, 
		f"{this_observer_uuid}/temp-v2/{x}/metadata.json").get()['Body'].read()) for x in unique_data_donation_uuids}
	elapsed_time = abs(int(time.time()) - elapsed_time)
	print(f"Time taken to index {len(data_donation_failsafe_cache.keys())} entries: ", elapsed_time, " seconds")

	true_positive_expanded_data_donations = list()
	for x in expanded_data_donations:
		this_platform = data_donation_metadata_cache[x["data_donation_uuid"]]["nameValuePairs"]["platform"]
		failsafe_obj = data_donation_failsafe_cache[x["data_donation_uuid"]][x["frame_n"]]
		is_true_positive = (any(failsafe_obj["ocr_frame_sponsored_evaluations"]) 
			or ((failsafe_obj["yolov5_detections"] is not None) and 
				(len(failsafe_obj["yolov5_detections"]) > 0) and
				(max([x["confidence"] for x in failsafe_obj["yolov5_detections"]]) > CONFIDENCE_THRESHOLD[this_platform]))) 
		if (is_true_positive):
			true_positive_expanded_data_donations.append(x)

	#with open("test_true_positive_expanded_data_donations.json", "w") as f: f.write(json.dumps(true_positive_expanded_data_donations,indent=3))
	#true_positive_expanded_data_donations = json.loads(open("test_true_positive_expanded_data_donations.json").read())

	'''

		of those that we do relate, line them up on a timeline and relate those frames together that

			a. are close time-wise
			AND
			b. have similar text prop positionings
			c. have matching perceptual hashes
		

		matching-ad-type
		strongly-close-time-wise
		weakly-close-time-wise
		strong-percetual-hashes
		weak-perceptual-hashes
		strong-ocr-overlaps
		weak-ocr-overlaps



		are connected if:

			matching-ad-type AND

				strongly-close-time-wise AND bounding boxes of composite clearly overlap

				OR

				weakly-close-time-wise AND strong-ocr-overlaps

	'''
	elapsed_time = int(time.time())
	grouped_timewise = list()
	current_group = list()
	relations = list()
	for x in true_positive_expanded_data_donations:
		relating_reason = None
		confidence_pct = None
		if (len(current_group) == 0):
			# Append if nothing within current group
			current_group.append(x)
		else:
			# Run a comparison between the last item of the last group
			last_frame = current_group[-1]
			current_frame = x
			are_connected, outcome = are_frames_connected(this_observer_uuid, last_frame, current_frame, data_donation_failsafe_cache, data_donation_metadata_cache)
			if (are_connected):
				current_group.append(x)
			else:
				grouped_timewise.append(list(current_group))
				current_group = [x]
			relations.append(outcome)
		if ((x == true_positive_expanded_data_donations[-1]) and (len(current_group) > 0)):
			grouped_timewise.append(current_group)

	elapsed_time = abs(int(time.time()) - elapsed_time)
	print(f"Time taken to run groupings:", elapsed_time, " seconds")

	'''
		for all groups, go through and keep reindexing until there are no more super-groups to form

		take a single group, and check it against all other candidates

		if a comparison is made between two groups, keep the result for future reference to avoid recomparing content

		as a note when comparing, if comparing a super group, always reference back to the exact frame being compared

		if a match is found between two frames, save the state of hte entire thing and start again
	'''
	comparison_dict = dict()
	super_grouping = True
	super_relations = list()
	grouped_timewise = [x for x in grouped_timewise if (len(x) > 0)]
	#print("len(grouped_timewise)", len(grouped_timewise))
	while (super_grouping):
		super_grouping = False
		for i in range(0, len(grouped_timewise)):
			for j in range(len(grouped_timewise)):
				try:
					last_frame = grouped_timewise[i][-1]
					current_frame = grouped_timewise[j][0]
					last_frame_id = frame_id(last_frame)
					current_frame_id = frame_id(current_frame)
					comparison_id = last_frame_id+"|"+current_frame_id
					if ((i != j) and (not comparison_id in comparison_dict) and (abs(last_frame["timestamp"] - current_frame["timestamp"]) <= N_TIME_GAP_WEAK)):
						comparison_dict[comparison_id] = True
						are_connected, outcome = are_frames_connected(this_observer_uuid, last_frame, current_frame, data_donation_failsafe_cache, data_donation_metadata_cache)
						super_relations.append(outcome)
						if (are_connected):
							super_grouping = True
							adjusted_group = list(grouped_timewise[i])
							adjusted_group.extend(grouped_timewise[j])
							grouped_timewise = [grouped_timewise[l] for l in range(len(grouped_timewise)) if (not l in [i,j])] + [adjusted_group]
							break
				except:
					print(traceback.format_exc())
					#ipdb.set_trace()
			if (super_grouping):
				break
	#print("len(grouped_timewise)", len(grouped_timewise))

	#print(json.dumps(grouped_timewise, indent=3))

	#with open("test_true_positive_expanded_data_donations.json", "w") as f: f.write(json.dumps(true_positive_expanded_data_donations,indent=3))
	#with open("relations.json", "w") as f: f.write(json.dumps(relations,indent=3))
	#with open("super_relations.json", "w") as f: f.write(json.dumps(super_relations,indent=3))
	#create_test_dir(this_observer_uuid, grouped_timewise)
	#ipdb.set_trace()
	formalized_log = {
			"grouped_timewise" : grouped_timewise,
			"relations" : relations,
			"expanded_data_donations" : expanded_data_donations,
			"super_relations" : super_relations
		}
	'''
		attach the completion flag to all relevant data donations involved in this analysis
	'''

	#create_test_dir(this_observer_uuid, grouped_timewise)
	data_donations_to_formalized_objs = dict()
	for this_group in grouped_timewise:
		# Determine the distinct data donation uuids that are relevant to this group
		unique_data_donation_uuids = list(set([x["data_donation_uuid"] for x in this_group]))
		observed_at_val = int(math.floor(min([entrypoint_cache[x]["observed_at"] for x in unique_data_donation_uuids])))
		# for each group, generate the formalized obj
		this_formalized_obj = [{
					"data_donation_uuid" : entry["data_donation_uuid"],
					"frame_observed_at" : entry["timestamp"],
					"observed_at" : observed_at_val,
					"frame" : entry["frame_n"]
				} for entry in this_group]
		# procure an insertion for the formalized cache
		# Update the cache sequentially
		formalized_cache = cache_read(this_observer_uuid, cache_name="formalized_cache", template_quick_access_cache=dict())
		this_uuid = str(uuid.uuid4())
		# Put the content of the group into an S3 object at the formalized sub-bucket
		s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/formalized/{this_uuid}.json').put(Body=json.dumps(this_formalized_obj, indent=3))
		formalized_cache[this_uuid] = dict()
		cache_write(this_observer_uuid, quick_access_cache=formalized_cache, cache_name="formalized_cache")
		# Get the backmappings
		for entry in this_group:
			if (not entry["data_donation_uuid"] in data_donations_to_formalized_objs):
				data_donations_to_formalized_objs[entry["data_donation_uuid"]] = list()
			data_donations_to_formalized_objs[entry["data_donation_uuid"]].append(this_uuid)
			data_donations_to_formalized_objs[entry["data_donation_uuid"]] = list(set(data_donations_to_formalized_objs[entry["data_donation_uuid"]]))
	# update the entrypoint cache to indicate that the formalized_v2 is complete
	entrypoint_cache = cache_read(this_observer_uuid, cache_name="entrypoint_cache", template_quick_access_cache=dict())
	for k in selected_entries:
		entrypoint_cache[k]["formalized_v2"] = int(time.time())
	# also run updates for backmappings
	for data_donation_uuid in data_donations_to_formalized_objs:
		if (not "formalized_v2_uuids" in entrypoint_cache[data_donation_uuid]):
			entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"] = list()
		entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"].extend(data_donations_to_formalized_objs[data_donation_uuid])
		entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"] = list(set(entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"]))
	cache_write(this_observer_uuid, quick_access_cache=entrypoint_cache, cache_name="entrypoint_cache")
	# apply the formalized log
	#s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f'{this_observer_uuid}/formalized_logs/{int(time.time())}.json').put(Body=json.dumps(formalized_log, indent=3))
	#ipdb.set_trace()
	#print(this_observer_uuid)
	#print(json.dumps(data_donations_to_formalized_objs,indent=3))


def routine_repair():
	# For every observer
	observer_uuids = ["e9821ce2-bb7b-4f5a-aed3-aa9967f2f0bb/"]#subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		try:
			data_donations_to_formalized_objs = dict()
			entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
			formalized_cache_path = f"{this_observer_uuid}/formalized_cache.json"
			if ((s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)) 
				and (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, formalized_cache_path))):
				entrypoint_cache = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path).get()['Body'].read())
				formalized_cache = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, formalized_cache_path).get()['Body'].read())
				# Go into their formalized folder and index all foramlized_uuid <-> data_donation_uuid relations
				for k in formalized_cache:
					# Load up the formalized_obj
					try:
						formalized_obj = json.loads(s3.Object(S3_BUCKET_MOBILE_OBSERVATIONS, f"{this_observer_uuid}/formalized/{k}.json").get()['Body'].read())
						for x in formalized_obj:
							x["data_donation_uuid"]
							if (not x["data_donation_uuid"] in data_donations_to_formalized_objs):
								data_donations_to_formalized_objs[x["data_donation_uuid"]] = list()
							data_donations_to_formalized_objs[x["data_donation_uuid"]].append(k)
							data_donations_to_formalized_objs[x["data_donation_uuid"]] = list(set(data_donations_to_formalized_objs[x["data_donation_uuid"]]))
					except:
						print("No key: "+k)
				#print(json.dumps(data_donations_to_formalized_objs,indent=3))
				# run updates for backmappings
				entrypoint_cache = cache_read(this_observer_uuid, cache_name="entrypoint_cache", template_quick_access_cache=dict())
				for data_donation_uuid in data_donations_to_formalized_objs:
					if (not "formalized_v2_uuids" in entrypoint_cache[data_donation_uuid]):
						entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"] = list()
					entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"].extend(data_donations_to_formalized_objs[data_donation_uuid])
					entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"] = list(set(entrypoint_cache[data_donation_uuid]["formalized_v2_uuids"]))
				cache_write(this_observer_uuid, quick_access_cache=entrypoint_cache, cache_name="entrypoint_cache")
			# Update the entrypoint_cache to reflect this
		except:
			print(traceback.format_exc())
			print("\tError")

# Run for no longer than 10 minutes
# 
N_SECONDS_TIMEOUT = (60 * 10)
def routine_batch(event, context=None):
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	random.shuffle(observer_uuids)
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		print(this_observer_uuid)
		entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
		time_at_call = int(time.time())
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)):
			routine_instance_v2(this_observer_uuid)
			print("Observer ", this_observer_uuid, " took ", abs(time_at_call - int(time.time())), " seconds")
			elapsed_time = abs(int(time.time()) - time_at_init)
			if (elapsed_time > N_SECONDS_TIMEOUT):
				print("Breaking to avoid overload at batch level...")
				break
	return str()

def routine_target(event, context=None):
	print("Target Event!!!")
	routine_instance_v2(event["observer_uuid"], 2500)

processes = { "batch" : routine_batch, "target" : routine_target }

def lambda_handler(event, context):
	#print(os.listdir(os.getcwd()))
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
	routine_repair()
	#routine_batch(dict())
	#routine_instance_v2("05b58b08-048c-4b2d-935f-c6bb542fd43d")
	#routine_instance_v2("1cf98df3-fa71-4074-9dcc-e95a222f51b3")



'''	


	
	reload tagging using backup folder


	formalzier on-demand will re-generate the entire formalized cache


	ea41b2e0-c8c6-4eea-b1d9-c6862c548b2f/


		formalized
			
			rdo_uuid.json

		quick_access_cache.json

		formalized_cache.json

		rdo

			timestamp.rdo_uuid

				output.json

		ccl

			timestamp.rdo_uuid

				advertiser_name_extraction.json

				scrape_instances.json

				scrape_uuids...

					scrape_output.json

					medias

						xxx.jpeg
'''

