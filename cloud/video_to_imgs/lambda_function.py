'''

	accepts the path to an mp4 within s3

	a destination to put images and log output

	and a followup event if necessary



	local test first

'''
import sys

if (__name__ != "__main__"):
	sys.path.insert(0,"/mnt/fs/")
else:
	import ipdb

import os
import json
import boto3
import botocore
import traceback
import cv2
import math
import time
import imagehash
from PIL import Image


##############################################################################################################################
##############################################################################################################################
### AWS
##############################################################################################################################
##############################################################################################################################

# Load up the necessary AWS infrastructure
# Note: On remote infrastructures, we don't authenticate as the Lambda handler will have the necessary
# permissions built into it
AWS_REQUIRED_RESOURCES = ["s3"]

META_ADLIBRARY_CONFIG = {
		"aws" : {
			"AWS_PROFILE" : "dmrc",
			"AWS_REGION" : "ap-southeast-2"
		}
	}

S3_OBSERVATIONS_BUCKET = "fta-mobile-observations-v2"

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
	credentials_applied = {
			"region_name" : META_ADLIBRARY_CONFIG["aws"]["AWS_REGION"]
		}
	AWS_RESOURCE = {k : boto3.resource(k, **credentials_applied) for k in AWS_REQUIRED_RESOURCES}
	AWS_CLIENT = {k : boto3.client(k, **credentials_applied) for k in AWS_REQUIRED_RESOURCES}
	return AWS_CLIENT, AWS_RESOURCE

AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))


##############################################################################################################################
##############################################################################################################################
### Utility Functions 
##############################################################################################################################
##############################################################################################################################



VIDEO_STILL_EXTRACTION_SCRIPT_CURRENT_VERSION = 4000

def load_mp4_resource(event):
	if (event["running_locally"]):
		return cv2.VideoCapture(event["source"])
	else:
		_, observer_uuid, subbucket, tentative_ad, video_uuid = event["source"].replace("s3://",str()).split("/")
		# Generate a presigned URL to the S3 bucket
		presigned_url = AWS_CLIENT["s3"].generate_presigned_url('get_object', 
			Params={'Bucket': S3_OBSERVATIONS_BUCKET, 'Key': f"{observer_uuid}/{subbucket}/{tentative_ad}/{video_uuid}"}, ExpiresIn=10000)
		print(presigned_url)
		return cv2.VideoCapture(presigned_url)

def get_frame_similarity_pct(img_sample, a, b):
	return (1 - (abs(img_sample[a]["phash"] - img_sample[b]["phash"])
				/ max(len(img_sample[a]["phash"]), len(img_sample[b]["phash"]))))


SIMILARITY_THRESHOLD = 0.70
def get_frame_relation(img_sample, a, b):
	similarity_pct = get_frame_similarity_pct(img_sample, a, b)
	verdict = ("SIMILAR" if (similarity_pct >= SIMILARITY_THRESHOLD) else "DIFFERENT")
	return { "verdict" : verdict, "similarity_pct" : similarity_pct }

def overlap(a, b):
	return (len(set(range(a[0],a[1]+1)).intersection(set(range(b[0],b[1]+1)))) > 0)

def strong_overlap(x, y):
	return max(x[0],y[0]) < min(x[1],y[1])

def collapse_ranges(ranges):
	tentative_ranges = list()
	for this_range in ranges:
		overlapping_ranges = [x for x in tentative_ranges if overlap(x, this_range)]
		if (len(overlapping_ranges) > 0):
			vals = this_range; [vals.extend(x) for x in overlapping_ranges]
			tentative_ranges = [x for x in tentative_ranges if (not x in overlapping_ranges)] + [[min(vals), max(vals)]]
		else:
			tentative_ranges.append(this_range)
	return tentative_ranges

def get_retained_frames(master_frame_similarity_readings):
	# Determine the frames to retain
	similarity_groups = list()
	retained_frames = list()
	last_reading_verdict = None
	for i in range(len(master_frame_similarity_readings)):
		this_reading = master_frame_similarity_readings[i]
		if (i == 0):
			retained_frames.append(this_reading["last_frame"])
		else:
			if ((this_reading["relation"]["verdict"] != last_reading_verdict) and (this_reading["relation"]["verdict"] == "DIFFERENT")):
				retained_frames.append(this_reading["last_frame"])
				retained_frames.append(this_reading["this_frame"])
		last_reading_verdict = this_reading["relation"]["verdict"]
		if (this_reading["relation"]["verdict"] == "SIMILAR"):
			this_reading_indices = [this_reading["last_frame"], this_reading["this_frame"]]
			applied = False
			for j in range(len(similarity_groups)):
				if (any([(this_reading[f"{x}_frame"] in similarity_groups[j]) for x in ["this", "last"]])):
					similarity_groups[j].extend(this_reading_indices)
					similarity_groups[j] = list(set(similarity_groups[j]))
					applied = True
			if (not applied):
				similarity_groups.append(this_reading_indices)

	inhibited_frames = list()
	for this_group in similarity_groups:
		this_intersection = sorted(list(set(this_group).intersection(set(retained_frames))))
		if (len(this_intersection) > 1):
			inhibited_frames.extend(this_intersection[:1])
	return list(set([x for x in retained_frames if (not x in inhibited_frames)])), similarity_groups
	'''
			if (this_reading["relation"]["verdict"] != last_reading_verdict):
				retained_frames.append(this_reading["last_frame"])
				retained_frames.append(this_reading["this_frame"])
				last_reading_verdict = this_reading["relation"]["verdict"]
	# Create similarity groups

	# Go over the retained frames - if any are jointly connected by a reading that is 'SIMILAR', remove one frame
	inhibited_frames = list()
	for this_reading in master_frame_similarity_readings:
		if (all([(x in retained_frames) for x in [this_reading["last_frame"], this_reading["this_frame"]]])
			and (this_reading["relation"]["verdict"] == "SIMILAR")):
			inhibited_frames.append(this_reading["last_frame"])
	return list(set([x for x in retained_frames if (not x in inhibited_frames)]))
	'''

'''
	The function works by firstly taking a shallow pass of the video (no more than a few equally spaced frames).

	Then it determines ranges in which the frames were not related, and iterates over them again, with a frame interval
	that is smaller.

	It does this repeatedly until a minimum frame interval threshold is reached.

	326445080_704462321214382_8506613622416987033_n.mp4_output
'''
N_SAMPLE_FRAMES = 5
FRAME_SAMPLE_LOWER_THRESHOLD = 2
def video_to_imgs(event, context=None):
	# Load in the MP4 video
	this_mp4_capture = load_mp4_resource(event)
	# Determine the total number of frames
	total_frames = int(this_mp4_capture.get(cv2.CAP_PROP_FRAME_COUNT))
	fps = float(this_mp4_capture.get(cv2.CAP_PROP_FPS))
	print("total_frames", total_frames)
	frame_sample_upper_threshold = math.floor(total_frames/N_SAMPLE_FRAMES)
	frame_sample_threshold_previous = None
	frame_sample_threshold = frame_sample_upper_threshold
	target_ranges = [[0, total_frames-1]]
	forced_retained_frames = list()
	img_sample = dict()
	master_frame_similarity_readings = list()
	contextualised_frame_similarity_readings = dict()
	while ((frame_sample_threshold >= FRAME_SAMPLE_LOWER_THRESHOLD) and (len(target_ranges) > 0)):
		print("Executing at frame sample threshold of ", frame_sample_threshold, " with ", len(target_ranges), " many target ranges")
		# Purge any readings that already exist within the master_frame_similarity_readings that may overlap the target ranges
		# (this is done to avoid overlap of frames)
		master_frame_similarity_readings = [x for x in master_frame_similarity_readings 
												if (not any([strong_overlap([x["last_frame"], x["this_frame"]], y) for y in target_ranges]))]
		if (frame_sample_threshold_previous is not None):
			contextualised_frame_similarity_readings[frame_sample_threshold] = [x for x in contextualised_frame_similarity_readings[frame_sample_threshold_previous] 
												if (not any([strong_overlap([x["last_frame"], x["this_frame"]], y) for y in target_ranges]))]
		#print(target_ranges)
		# Run sampling over target ranges - for each target range, produce the readings
		frame_similarity_readings = list()
		print(target_ranges)
		for this_range in target_ranges:
			# Go over the frames within this range
			last_frame = None
			this_range_readings = list()
			for _this_frame in range(this_range[0], this_range[1]+frame_sample_threshold, frame_sample_threshold):
				this_frame = _this_frame
				if (this_frame >= total_frames):
					this_frame = total_frames-1
				if (this_frame > this_range[1]):
					this_frame = this_range[1]
				if (not this_frame in img_sample):
					this_mp4_capture.set(cv2.CAP_PROP_POS_FRAMES, this_frame)
					_, img = this_mp4_capture.read()
					img_sample[this_frame] = {
							"img" : img,
							"phash" : imagehash.phash(Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB)))
						}
				if (last_frame is not None):
					this_reading = {
							"last_frame" : last_frame,
							"this_frame" : this_frame,
							"relation" : get_frame_relation(img_sample, last_frame, this_frame)
						}
					frame_similarity_readings.append(this_reading)
					this_range_readings.append(this_reading)
				last_frame = this_frame
			# If a range is entirely similar (from all its sub-comparisons), yet has become the subject of comparison (from being different in a
			# comparison of a larger frame similarity threshold, then we must place the boundaries of the range into a 'forced' list of retained frames)
			#print(forced_retained_frames)
			if (not any([(x["relation"]["verdict"] == "DIFFERENT") for x in this_range_readings])):
				forced_retained_frames.extend(this_range)

		# TODO do not collapse the ranges, as this forces the deletion of valuable data relating to ranges that may harbour differences
		# consider two adjacent ranges that are collapsed into one range - where at a lower frame threshold, no difference is found in the second range
		# the frame differnce at the hiher frame threshold is lost
		#print(frame_similarity_readings)
		# Note: While we would typically reorder the readings (in frame_similarity_readings by frames) at this stage, the
		# iteration process handles it automatically for us
		#
		# Establish new ranges to index
		ranges_to_collapse = [[this_reading["last_frame"], this_reading["this_frame"]] 
																for this_reading in frame_similarity_readings
																			if (this_reading["relation"]["verdict"] == "DIFFERENT")]
		#print("ranges_to_collapse", ranges_to_collapse)
		target_ranges = ranges_to_collapse # TODO#collapse_ranges(ranges_to_collapse)
		#print("target_ranges", target_ranges)
		# Halve the frame_sample_threshold
		frame_sample_threshold = math.floor(frame_sample_threshold/2)

		# Record the frame similarity readings if they are either well-formed, or if the frame similarity readings have been calculated
		# for the minimum threshold
		list_to_apply = [x for x in frame_similarity_readings 
			if ((x["relation"]["verdict"] != "DIFFERENT") or (frame_sample_threshold < FRAME_SAMPLE_LOWER_THRESHOLD))]
		master_frame_similarity_readings.extend(list_to_apply)
		contextualised_frame_similarity_readings[frame_sample_threshold] = frame_similarity_readings
		frame_sample_threshold_previous = frame_sample_threshold
	# Order the results
	master_frame_similarity_readings = sorted(master_frame_similarity_readings, key=lambda d: d['last_frame'])

	# Retrieve the retained frames
	retained_frames, similarity_groups = get_retained_frames(master_frame_similarity_readings)

	contextualised_retained_frames = dict()
	for k in contextualised_frame_similarity_readings:
		contextualised_frame_similarity_readings[k] = sorted(contextualised_frame_similarity_readings[k], key=lambda d: d['last_frame'])
		contextualised_retained_frames[k], _ = get_retained_frames(contextualised_frame_similarity_readings[k])
	#ipdb.set_trace()
	#contextualised_retained_frames[k]
	retained_frames = list(set(retained_frames + forced_retained_frames))
	output = {
			"source" : event["source"],
			"retained_frames" : retained_frames,
			"contextualised_retained_frames" : contextualised_retained_frames,
			"contextualised_frame_similarity_readings" : contextualised_frame_similarity_readings,
			"similarity_readings" : master_frame_similarity_readings,
			"n_frames" : total_frames,
			"similarity_groups" : similarity_groups,
			"fps" : fps,
			"script_version" : VIDEO_STILL_EXTRACTION_SCRIPT_CURRENT_VERSION
		}

	if (event["running_locally"]):
		outputs_dir = os.path.join(os.getcwd(), "test_videos_outputs")
		try: os.mkdir(outputs_dir)
		except: pass
		this_output_dir = os.path.join(outputs_dir, event["source"].split("/")[-1]+"_output")
		try: os.mkdir(this_output_dir)
		except: pass

		for this_frame in retained_frames:
			cv2.imwrite(os.path.join(this_output_dir, str(this_frame)+".jpg"), img_sample[this_frame]["img"])
		
		with open(os.path.join(this_output_dir, "output.json"), "w") as f:
			f.write(json.dumps(output, indent=3))
			f.close()
	else:
		this_prefix = event["destination"].replace(f"s3://{S3_OBSERVATIONS_BUCKET}/", str())
		for this_frame in retained_frames:
			AWS_RESOURCE["s3"].Bucket(S3_OBSERVATIONS_BUCKET).put_object(
				Key=f"{this_prefix}{this_frame}.jpg", 
				Body=cv2.imencode('.jpg', img_sample[this_frame]["img"])[1].tobytes(), ContentType='image/jpeg')
		AWS_RESOURCE["s3"].Object(S3_OBSERVATIONS_BUCKET, 
			f"{this_prefix}output.json").put(Body=(bytes(json.dumps(output,indent=3).encode('UTF-8'))))

def lambda_handler(event, context):
	print("ACTION RECEIVED:", event["action"])
	commands = { 
			"video_to_imgs" : video_to_imgs
		}
	return {
			'statusCode': 200,
			'body': (placeholder 
				if (not event["action"] in commands) else commands[event["action"]])(event, context=context)
		}

if (__name__ == "__main__"):
	if (sys.argv[1] == "test_videos"):
		for x in os.listdir(os.path.join(os.getcwd(), "test_videos")):
			lambda_handler({
					"action" : "video_to_imgs",
					"running_locally" : True,
					"source" : os.path.join(os.getcwd(), "test_videos", x),
					"destination" : None
				}, None)
	else:
		lambda_handler({
				"video_to_imgs" : {
					"action" : "video_to_imgs",
					"running_locally" : True,
					"source" : os.path.join(os.getcwd(), "test_videos", "429916752_6946528508809441_4313508713794936245_n.mp4"),
					"destination" : None
				},
				"video_to_imgs_remote" : {
					"action" : "video_to_imgs",
					"running_locally" : False,
					"source" : "s3://fta-mobile-observations-v2/471c82ad-a8dd-4a1e-be5c-3a23ce725ddc/meta_adlibrary_scrape/1727278028490.7867e336-89f3-4b58-8c7f-cf6870552b63/b981eac8-479b-4813-9fcb-66818189007d.mp4",#"s3://fta-mobile-observations-v2/f2c30899-7e75-4e39-bf51-75e9d7fc2926/meta_adlibrary_scrape/1726708625244.b0a6fb2e-1344-4b03-ac67-679058130780/fc31d2a0-a7bf-495c-a34a-1d9f8e11e715.mp4",
					"destination" : "s3://fta-mobile-observations-v2/471c82ad-a8dd-4a1e-be5c-3a23ce725ddc/meta_adlibrary_scrape/1727278028490.7867e336-89f3-4b58-8c7f-cf6870552b63/b981eac8-479b-4813-9fcb-66818189007d.mp4.to_imgs/"#"s3://fta-mobile-observations-v2/f2c30899-7e75-4e39-bf51-75e9d7fc2926/meta_adlibrary_scrape/1726708625244.b0a6fb2e-1344-4b03-ac67-679058130780/fc31d2a0-a7bf-495c-a34a-1d9f8e11e715.mp4.to_imgs/"
				}
			}[sys.argv[1]], None)





