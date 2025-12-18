'''

 
consider a bunch of ocrs attached to an observation

an advertiser term is retrived from the ocrs and mapped accordingly

	the term is sensitive to where in the ads geometry, the sponsored term sits, and where then we can expect to derive the advertiser name

	it then retrieves the advertiser name by pulling together the isolated OCRs, and forming a chatgpt request to isolate the desired content

this creates a metadata structure over the observations with indications of what advertiser term is ascribed to what


'''

import sys
import os
if (__name__ == "__main__"):
	import ipdb
import random
import json
from io import BytesIO
import time
import json
import math
import uuid
import boto3
import botocore
import traceback
import base64
import requests
from rect_overlaps_min import *
from ocr_bbox_operations import *
from sliding_levenshtein import *
from numericals import *
from distributed_cache import *
s3_client = boto3.client('s3', region_name='ap-southeast-2')

s3 = boto3.resource('s3')

S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"
S3_BUCKET_MOBILE_OBSERVATIONS_CCL = "fta-mobile-observations-v2-ccl"

VERBOSE = False

SCRAPE_THRESHOLD_INTERVAL = 3 * 24 * 60 * 60

S3_BUCKET_MOBILE_OBSERVATIONS = "fta-mobile-observations-v2"

CONFIDENCE_THRESHOLD = {
		"FACEBOOK" : 0.05,
		"INSTAGRAM" : 0.40,
		"TIKTOK" : 0.50,
		"YOUTUBE" : 0.50
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

def s3_dict_read(this_bucket, this_key, default=dict()):
	try:
		return json.loads(s3.Object(this_bucket, this_key).get()['Body'].read())
	except:
		return dict()

def s3_dict_write(this_bucket, this_key, content):
	s3.Object(this_bucket, this_key).put(Body=json.dumps(content, indent=3))




def load(path, this_bucket=S3_BUCKET_MOBILE_OBSERVATIONS):
	return json.loads(s3.Object(this_bucket, path).get()['Body'].read())


'''
	This function retrieves the relevant segments of OCR on a single frame of a data donation
'''
def relevant_ocrs_on_observation_frame(observer_uuid, data_donation_uuid, this_frame, data_donation_metadata):
	this_ocr_unfiltered = load(f"{observer_uuid}/temp-v2/{data_donation_uuid}/{this_frame}.jpg.ocr.json")
	# Set a minimum threshold to regard texts
	min_threshold_regard_ocr = 0.25
	this_ocr = [x for x in this_ocr_unfiltered if (x["confidence"] > min_threshold_regard_ocr)]
	# Get the ad type and platform
	this_platform = data_donation_metadata["nameValuePairs"]["platform"]
	this_ad_type = data_donation_metadata["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][this_frame]["internalJSONObject"]["nameValuePairs"]["adType"]
	# Retrieve the screen dimensions associated with the frame
	screen_dimensions = data_donation_metadata["nameValuePairs"]["systemInformation"]["internalJSONObject"]["nameValuePairs"]["screenDimensions"]["internalJSONObject"]["nameValuePairs"]
	screen_dimensions = {z:int(screen_dimensions[z]) for z in ["w", "h"]}
	# We need to determine (for the given frame) what the bounds were for the 'Sponsored' text
	this_frame_obj = data_donation_metadata["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][this_frame]["internalJSONObject"]["nameValuePairs"]
	bbox_cropped = this_frame_obj["inference"]["internalJSONObject"]["nameValuePairs"]["boundingBoxCropped"]["internalJSONObject"]["nameValuePairs"]
	bbox_sponsored = this_frame_obj["inference"]["internalJSONObject"]["nameValuePairs"]["boundingBoxSponsored"]["internalJSONObject"]["nameValuePairs"]
	

	# This adjusts the bbox coordinates of the 'Sponsored' text so that they can be adequately compared with the OCR
	bbox_sponsored_contextualised = {
			"x1" : bbox_sponsored["x1"] - bbox_cropped["x1"],
			"x2" : bbox_sponsored["x2"] - bbox_cropped["x1"],
			"y1" : bbox_sponsored["y1"] - bbox_cropped["y1"],
			"y2" : bbox_sponsored["y2"] - bbox_cropped["y1"],
			"cx" : bbox_sponsored["cx"] - bbox_cropped["x1"],
			"cy" : bbox_sponsored["cy"] - bbox_cropped["y1"],
			"w" : bbox_sponsored["w"],
			"h" : bbox_sponsored["h"],
			"className" : bbox_sponsored["className"],
		}
	# Convert the bbox to an OCR rectangle (as the next step before comparison)
	bbox_sponsored_contextualised_ocr = {
			"x" : int(math.floor(bbox_sponsored_contextualised["x1"] * screen_dimensions["w"])),
			"y" : int(math.floor(bbox_sponsored_contextualised["y1"] * screen_dimensions["h"])),
			"w" : int(math.floor(bbox_sponsored_contextualised["w"] * screen_dimensions["w"])),
			"h" : int(math.floor(bbox_sponsored_contextualised["h"] * screen_dimensions["h"]))
		}



	# For geometric operations
	sponsored_ocr_rect = ocr_to_rect(bbox_sponsored_contextualised_ocr)
	ocr_rects = [ocr_to_rect(x) for x in this_ocr]
	relevant_ocrs = list()
	if (((this_platform == "FACEBOOK") and (this_ad_type in ["MARKETPLACE_BASED", "FEED_BASED", "STORY_BASED"]))
		or ((this_platform == "INSTAGRAM") and (this_ad_type in ["FEED_BASED"]))):
		# Only regard texts that sit above the 'Sponsored term'
		#
		# Specifically, any OCRs whose:
		#   * starting x larger than that of the bbox_sponsored_contextualised_ocr minus 25% of its width
		#	* starting y is smaller than that of the bbox_sponsored_contextualised_ocr
		#
		starting_x_threshold = bbox_sponsored_contextualised_ocr["x"] - (bbox_sponsored_contextualised_ocr["w"] * 0.25)
		ending_y_threshold = bbox_sponsored_contextualised_ocr["y"]
		starting_y_threshold = ending_y_threshold - (bbox_sponsored_contextualised_ocr["h"] * 5)
		#
		for i in range(len(this_ocr)):
			candidate_ocr = this_ocr[i]
			if ((candidate_ocr["x"] > starting_x_threshold) 
				and (candidate_ocr["y"] < ending_y_threshold) and (candidate_ocr["y"] > starting_y_threshold)):
				relevant_ocrs.append(this_ocr[i])
		#
	if (((this_platform == "FACEBOOK") and (this_ad_type in ["REEL_BASED"]))
		or ((this_platform == "INSTAGRAM") and (this_ad_type in ["REEL_BASED"]))):
		# Search for a term that resides above the Sponsored term, but whose y is in the lower third of the 
		# screen dimensions (strictly, as sometimes we get cut offs), and whose starting x is within the
		# bound of the 'Sponsored' text term
		#
		# Furthermore, accept only the candidate with the largest x offset
		max_x_offset = None
		starting_x_threshold = bbox_sponsored_contextualised_ocr["x"]
		ending_x_threshold = bbox_sponsored_contextualised_ocr["x"] + (bbox_sponsored_contextualised_ocr["w"] * 1.5)
		starting_y_threshold_pct = 0.66
		ending_y_threshold = bbox_sponsored_contextualised_ocr["y"]
		#
		for i in range(len(this_ocr)):
			# Take the offset of the bbox_cropped and add it to the y of the OCR (contextualised to the bbox_cropped as a percentage)
			ocr_y_pct = (this_ocr[i]["y"] / int(math.floor(bbox_cropped["h"] * screen_dimensions["h"]))) + bbox_cropped["y1"]
			candidate_ocr = this_ocr[i]
			if ((candidate_ocr["x"] > starting_x_threshold) and (candidate_ocr["x"] <= ending_x_threshold) 
				and (ocr_y_pct > starting_y_threshold_pct) and (candidate_ocr["y"] < ending_y_threshold)):
				if (max_x_offset is None) or (candidate_ocr["x"] > max_x_offset):
					max_x_offset = candidate_ocr["x"]
					relevant_ocrs = [this_ocr[i]]
		#
	if ((this_platform == "FACEBOOK") and (this_ad_type in ["REEL_FOOTER_BASED"])):
		# We have yet to consistently observe the advertiser name appearing anywhere except adjacent to the Sponsored term.
		# When terms do not appear adjacent, they are not the advertiser name.
		height_fraction = bbox_sponsored_contextualised_ocr["h"] * 0.25
		starting_y_threshold = (bbox_sponsored_contextualised_ocr["y"] - height_fraction)
		ending_y_threshold = ((bbox_sponsored_contextualised_ocr["y"] + bbox_sponsored_contextualised_ocr["h"]) + height_fraction)
		starting_x_threshold_negation = bbox_sponsored_contextualised_ocr["x"]
		ending_x_threshold_negation = (bbox_sponsored_contextualised_ocr["x"] + bbox_sponsored_contextualised_ocr["w"])
		#
		for i in range(len(this_ocr)):
			candidate_ocr = this_ocr[i]
			if (((candidate_ocr["x"] < starting_x_threshold_negation) or (candidate_ocr["x"] > ending_x_threshold_negation))
				and (candidate_ocr["y"] > starting_y_threshold) and (candidate_ocr["y"] < ending_y_threshold)):
				relevant_ocrs.append(this_ocr[i])
		#
	if ((this_platform == "INSTAGRAM") and (this_ad_type == "STORY_BASED")):
		# Irrespective of where the 'Sponsored' term is, grab the OCR whose x+w is closest to the right (to avoid grabbing logo-based texts)
		# and whose y is in the first 10% of the y on the page
		ending_y_threshold = screen_dimensions["h"] * 0.10
		max_x_offset = None
		for i in range(len(this_ocr)):
			candidate_ocr = this_ocr[i]
			if ((candidate_ocr["y"] + candidate_ocr["h"]) <= ending_y_threshold):
				this_x_offset = (candidate_ocr["x"] + candidate_ocr["w"])
				if ((max_x_offset is None) or (this_x_offset > max_x_offset)):
					max_x_offset = this_x_offset
					relevant_ocrs = [this_ocr[i]]
		#
	if ((this_platform == "TIKTOK") and (this_ad_type == "THUMBNAIL")):
		# Left-most bottom candidate
		# To get the bottom candidate, isolate all texts whose h x 2 offset from its y extend beyond the screen h
		tentative_relevant_ocrs = list()
		for i in range(len(this_ocr)):
			candidate_ocr = this_ocr[i]
			if ((candidate_ocr["y"] + (candidate_ocr["h"] * 2)) > (screen_dimensions["h"] * bbox_cropped["h"])):
				tentative_relevant_ocrs.append(candidate_ocr)
		relevant_ocrs = [x for x in [min(tentative_relevant_ocrs, key=lambda d: d.get("x"), default=None)] if (x is not None)]
		#
	if ((this_platform == "TIKTOK") and (this_ad_type in ["REEL_FROM_SEARCH", "REEL_FROM_HOME"])):
		# Go to bottom 25% of y and get upper-left-most candidate whose x sits in 1/13th of x
		tentative_relevant_ocrs = list()
		for i in range(len(this_ocr)):
			candidate_ocr = this_ocr[i]
			if ((candidate_ocr["y"] > (screen_dimensions["h"] * bbox_cropped["h"] * 0.66))
				and (candidate_ocr["x"] < (screen_dimensions["w"] * bbox_cropped["w"] * (1 / 12)))):
				tentative_relevant_ocrs.append(candidate_ocr)
		# Isolating minimum y
		relevant_ocrs = [x for x in [min(tentative_relevant_ocrs, key=lambda d: d.get("y"), default=None)] if (x is not None)]
		#
	if ((this_platform == "YOUTUBE") and (this_ad_type in ["GENERAL_FEED_BASED", "PREVIEW_PORTRAIT_BASED", "REEL_BASED"])):
		sponsored_term_ocrs = list()
		non_sponsored_term_ocrs = list()
		for i in range(len(this_ocr)):
			candidate_ocr = this_ocr[i]
			if (sliding_levenshtein_pct(candidate_ocr["text"], "Sponsored") > 0.75):
				sponsored_term_ocrs.append(candidate_ocr)
			else:
				non_sponsored_term_ocrs.append(candidate_ocr)
		if (this_ad_type in ["PREVIEW_PORTRAIT_BASED", "REEL_BASED"]):
			# Find the inner-most 'Sponsored' term - if there is an advertiser name directly above it, we can use that
			for x in non_sponsored_term_ocrs:
				for this_sponsored_term_ocr in sponsored_term_ocrs:
					if ((x["y"] < this_sponsored_term_ocr["y"]) 
						and (abs(x["x"] - this_sponsored_term_ocr["x"]) < (this_sponsored_term_ocr["w"] * 0.2))):
							relevant_ocrs.append(x)
							break
			pass
		if (this_ad_type in ["GENERAL_FEED_BASED"]):
			# Find the term that appears directly aside the 'Sponsored' term (if applicable)
			for x in non_sponsored_term_ocrs:
				for this_sponsored_term_ocr in sponsored_term_ocrs:
					if ((x["x"] > this_sponsored_term_ocr["x"]) 
						and (abs(x["y"] - this_sponsored_term_ocr["y"]) < (this_sponsored_term_ocr["h"] * 0.2))):
							relevant_ocrs.append(x)
							break




	# TODO - YOUTUBE - APP_FEED_BASED ads have been inadequately retrieved
	# TODO - YOUTUBE - PRODUCT_FEED_BASED ads are not discernable - no consistent way to retrieve advertiser
	# TODO - YOUTUBE - PREVIEW_LANDSCAPE_BASED ads are non-discernable - due to technical errors
	#relevant_ocrs_stitched = stitch_ocr_boxes(relevant_ocrs, join_lines_with=" ")
	stitched_ocr_boxes_results = stitch_lines_then_blocks(relevant_ocrs)
	#ipdb.set_trace()
	stitched_ocr_extracted_terms = [{y:x[y] for y in ["text", "confidence"]} for x in stitched_ocr_boxes_results["blocks"]]
	#ipdb.set_trace()
	#stitched_ocr_extracted_terms = stitch_ocr_boxes_lines(stitched_ocr_boxes_results)

	#. Post-processing corrections

	if ((this_platform == "FACEBOOK") and (this_ad_type == "FEED_BASED")):
		# Remove 'x' terms which are mistaken crosses, and 'join' terms which are contaminations from page-based posts mistaken as ads
		stitched_ocr_extracted_terms = [x for x in stitched_ocr_extracted_terms if (not x["text"].lower() in ["x", "join"])]
		# For partnership ads, apply both terms (as we can't assume which is the advertiser by order)
		for x in stitched_ocr_extracted_terms:
			if ("with" in x):
				stitched_ocr_extracted_terms.extend([{"text" : y.strip(), "confidence" : x["confidence"]} for y in x["text"].split("with")])

	if ((this_platform == "FACEBOOK") and (this_ad_type == "REEL_BASED")):
		# In some cases, a feed-based ad is misclassified as a reel ad - this leads to substantially more extracted OCRs than intended - we
		# counter this by disincluding the entire frame
		max_n_ocr_terms_on_reel = 3
		if (len(stitched_ocr_extracted_terms) > max_n_ocr_terms_on_reel):
			stitched_ocr_extracted_terms = list()

	if ((this_platform == "FACEBOOK") and (this_ad_type == "REEL_FOOTER_BASED")):
		# Footer-based ads often collapse the 'Sponsored' term into the OCR that retrieves the advertiser name.
		# To overcome this, we remove the 'Sponsored' term (and divider if applicable)
		stitched_ocr_extracted_terms_processed = list()
		for x in stitched_ocr_extracted_terms:
			if ("Sponsored" in x["text"]):
				adjusted_x = x["text"].replace("Sponsored", str())
				adjusted_x_split = [y for y in adjusted_x.split(" ") if (len(y) > 0)]
				# Remove the dot if applicable
				if (len(adjusted_x_split) > 0):
					if (len(adjusted_x_split[-1]) == 1):
						adjusted_x_split = adjusted_x_split[:-1]
					stitched_ocr_extracted_terms_processed.append({"text" : " ".join(adjusted_x_split), "confidence" : x["confidence"]})
			else:
				stitched_ocr_extracted_terms_processed.append(x)
		stitched_ocr_extracted_terms = stitched_ocr_extracted_terms_processed

	if ((this_platform == "INSTAGRAM") and (this_ad_type == "FEED_BASED")):
		# Remove 'x' terms which are mistaken crosses, and 'join' terms which are contaminations from page-based posts mistaken as ads
		stitched_ocr_extracted_terms = [x for x in stitched_ocr_extracted_terms if (not x["text"].lower() in ["follow"])]

	#if any([(x["text"] == "JCU: James Cook University;") for x in relevant_ocrs]):
	#	print("got here 2")
	#	ipdb.set_trace()
	return {
			"this_platform" : this_platform,
			"this_ad_type" : this_ad_type,
			"relevant_ocrs" : relevant_ocrs,
			"relevant_ocrs_stitched" : stitched_ocr_boxes_results,
			"relevant_ocrs_extracted_terms" : stitched_ocr_extracted_terms
		}



'''
	
		create a ccl identity that maps back to a collection of data donations

			the ccl identity consists of:

				* the raw candidates of text

				* the 'best candidate' terms

				TODO - take a sample of data and see how often it returns the correct data

	This function takes the true_positive_expanded_data_donations, examining all observed ccl terms and using them to form
	best candidate terms - this is done as follows:

		1. The first step involved in this process is grouping together terms - this can be done from a good indication of time separation
		between terms, but also relies on sliding levenshteins to get a good indication of relatedness - conditions of relation are as follows:

			a. Terms must be within a 5 second window of each other

			b. Terms must be strongly related on a sliding levenshtein distance

			c. Terms must be same platform and ad type

		2. With the groups, we cross-relate terms to align them and get indications of replacements between characters where applicable - this
		then derives the best candidate terms.

			Note: Sometimes there won't be a strong candidate, and we should flag this.

		3. We summarize the best candidate terms, giving a clear indication of which data donations they map back to, in preparation for the
		scraping procedures - as an added aspect to aid with indexing (and to prevent the ccl identifier from having 'back and forth'), we insert
		the procured records into the separate ad library scrape bucket.
'''
def grouped_terms_from_data_donations(observer_uuid, true_positive_expanded_data_donations):
	MIN_LEVENSHTEIN_SIMILARITY_THRESHOLD_PCT = 0.80
	MIN_TIMESTAMP_DIFFERENCE_SECONDS = 300
	grouped_terms = list()
	'''
	# Uncomment for demonstration of step 2
	true_positive_expanded_data_donations[0]["relevant_ocrs"]["relevant_ocrs_extracted_terms"].extend([
			{
				"text": "MeDakaroony",
				"confidence": 0.9953872019232617
			},
			{
				"text": "MelakarooDyaa",
				"confidence": 0.9953872019232617
			},
			{
				"text": "aMelDkaroony",
				"confidence": 0.9953872019232617
			},
			{
				"text": "MelakarDonya",
				"confidence": 0.9953872019232617
			}
		])
	'''
	# Step 1.
	for x in true_positive_expanded_data_donations:
		for y in x["relevant_ocrs"]["relevant_ocrs_extracted_terms"]:
			tentative_member = {
					"timestamp" : x["timestamp"],
					"observer_uuid" : observer_uuid,
					"data_donation_uuid" : x["data_donation_uuid"],
					"platform" : x["relevant_ocrs"]["this_platform"],
					"ad_type" : x["relevant_ocrs"]["this_ad_type"],
					"text" : y["text"],
					"confidence" : y["confidence"]
				}
			grouped = False
			for this_group in grouped_terms:
				for i in range(len(this_group["members"])):
					existing_member = this_group["members"][i]
					c_timestamp = (abs(tentative_member["timestamp"] - existing_member["timestamp"]) <= MIN_TIMESTAMP_DIFFERENCE_SECONDS)
					c_platform_ad_type = all([(tentative_member[z] == existing_member[z]) for z in ["platform", "ad_type"]])
					if ((c_timestamp) and (c_platform_ad_type)):
						sliding_levenshtein_pcts = sliding_levenshtein_pct_annotated(existing_member["text"], tentative_member["text"], 3)
						if ((len(sliding_levenshtein_pcts) > 0) and (max(sliding_levenshtein_pcts) > MIN_LEVENSHTEIN_SIMILARITY_THRESHOLD_PCT)):
							this_group["similarities"].append({
									"i" : i,
									"j" : len(this_group["members"]),
									"sliding_levenshtein_pcts" : sliding_levenshtein_pcts
								})
							this_group["members"].append(tentative_member)
							grouped = True
							break
				if (grouped):
					break
			# If not grouped, create a new group
			if (not grouped):
				grouped_terms.append({
						"similarities" : list(),
						"members" : [tentative_member]
					})
	# Step 2.
	# For each group, the terms are cross-examined - the smaller terms are dragged along the big terms to form offsets
	# Then, going along each character, determine the best candidate character based on the confidence of the sliding levenshtein
	for this_group in grouped_terms:
		this_group["offset_map"] = list()
		if (len(this_group["similarities"]) > 0):
			for h in range(len(this_group["members"])):
				this_member = this_group["members"][h]
				for s in this_group["similarities"]:
					if (s["i"] == h):
						this_group["offset_map"].append({
								"offset" : int(),
								"i" : h
							})
						break
					elif (s["j"] == h):
						offset_of_i = [x for x in this_group["offset_map"] if (x["i"] == s["i"])][0]["offset"]
						this_offset_to_i = s["sliding_levenshtein_pcts"].index(max(s["sliding_levenshtein_pcts"]))
						this_group["offset_map"].append({
								"offset" : (this_offset_to_i + offset_of_i),
								"i" : h
							})
						break
		else: this_group["offset_map"] = [{"offset" : int(), "i" : int()}]
		this_group["offset_map"] = sorted(this_group["offset_map"], key=lambda x: x["offset"])
		this_group["offset_map_stringified"] = list()
		# Generate offset strings
		global_offset = int()
		for x in this_group["offset_map"]:
			if (x["offset"] > global_offset):
				offset_difference = abs(global_offset - x["offset"])
				global_offset = x["offset"]
				# All elements in list get adjusted by difference
				for i in range(len(this_group["offset_map_stringified"])):
					this_group["offset_map_stringified"][i]["text"] = (" " * offset_difference) + this_group["offset_map_stringified"][i]["text"]
			# Current element is extended by raw amount
			this_group["offset_map_stringified"].append({
					"text" : (" " * global_offset) + this_group["members"][x["i"]]["text"],
					"confidence" : this_group["members"][x["i"]]["confidence"]
				})
		# Add trailing characters to all strings
		max_characters = max([len(x["text"]) for x in this_group["offset_map_stringified"]])
		for i in range(len(this_group["offset_map_stringified"])):
			n_characters = len(this_group["offset_map_stringified"][i]["text"])
			add_characters = max_characters - n_characters
			this_group["offset_map_stringified"][i]["text"] += (" " * add_characters)

		'''
			Case 1: high probability on single element that is aligned to nulls
			Case 2: high probability on single character that is aligned to 

			for a single column, there are distinct characters and reoccurrences

			we need a factor to determine the weight that a reoccurrence has on a distinct character

			resorting to noisy-or
		'''
		# Aligning the data, each index has a set of characters, where each has a confidence
		# For each character in offset string list, run comparisons
		this_group["reweighted_term_characters"] = list()
		for character_i in range(len(this_group["offset_map_stringified"][0]["text"])):
			candidates = [{"value" : x["text"][character_i], "confidence" : x["confidence"]} for x in this_group["offset_map_stringified"]]
			this_group["reweighted_term_characters"].append(aggregate_confidences(candidates, method="noisy_or", normalize=True))
		this_group["reweighted_term"] = str().join([x[0]["value"] for x in this_group["reweighted_term_characters"]]).strip()
		
		# TODO - Perform an initial 'stray text' removal
		'''
			Split the string into parts

			Remove anything that doesn't resemble a typical string

				i.e. very few characters or predominantly non-alphanumerical
			
			Rejoin string
		'''
		parts = this_group["reweighted_term"].split(" ")
		this_group["reweighted_term_alphabetic_focused"] = " ".join([x for x in parts if (alpha_percentage(x) > 0.5)])
		this_group["reweighted_term_alphabetic_focused"] = None if (len(this_group["reweighted_term_alphabetic_focused"]) == 0) else this_group["reweighted_term_alphabetic_focused"]
		# Derive the splittable strings
		# String is broken into all parts that will be queried
		SPLITTING_TERMS = [" and ", " with "]
		this_group["reweighted_term_splittings"] = list()
		if (this_group["reweighted_term_alphabetic_focused"] is not None):
			this_group["reweighted_term_splittings"].append(this_group["reweighted_term_alphabetic_focused"])
			for x in SPLITTING_TERMS:
				if (x in this_group["reweighted_term_alphabetic_focused"]):
					this_group["reweighted_term_splittings"].extend(this_group["reweighted_term_alphabetic_focused"].split(x))
		# Make distinct
		this_group["reweighted_term_splittings"] = list(set(this_group["reweighted_term_splittings"]))
		# Remove any terms that look negligible
		NEGLIGIBLE_TERMS = ["X", "Follow"]
		this_group["reweighted_term_splittings"] = [x for x in this_group["reweighted_term_splittings"] if (not any([(sliding_levenshtein_pct(x,y, MIN_QUERY_STRING_LENGTH=1) > 0.80) for y in NEGLIGIBLE_TERMS]))]
		# Remove any terms that exceed 64 characters
		this_group["reweighted_term_splittings"] = [x for x in this_group["reweighted_term_splittings"] if (len(x) <= 64)]

	# This data helps us determine the degree to which the terms have been able to derive queries
	# - note that this is only a percentage of the 'groups' that could be covered, and not an indication
	# of the coverage of data donations, or ads themselves, which happen in a separate stream of analytics
	#n_distinct_terms_derived = len([x["reweighted_term_splittings"] for x in grouped_terms if (len(x["reweighted_term_splittings"]) > 0)])
	#n_distinct_terms = len(grouped_terms)
	#pct_distinct_terms_derived = (n_distinct_terms_derived / n_distinct_terms)
	return grouped_terms
	'''
		For each grouped term, a query object is generated - the object will be responsible for
		connecting the data donations to the query, to the response for the query.

		ccl_cache <- indexes the ccl queries for processing and also provides details about retrieving grouped_terms info on the query

		ccl_data_donation_cache <- easily maps data donations to ccl queries
	'''
	#ccl_data_donation_cache
	#cache_read(this_observer_uuid, cache_name="quick_access_cache", template_quick_access_cache=dict())
	# A query is mapped 
	'''

		Each group details the relationship shared between the data donations and the query

		Hereafter, all affected data donations are indexed for having completed ccl_term_identified

	'''
	# Finally, all candidates are sent off and will be compared against the commercial content library to determine the final results
	# of comparison
	# From sample, how many are correct/incorrect?

'''
	This function constructs a test that takes the Ad Observatory dashboard path - assessing the first frame of the observation
	to them form a relevant OCR result

'''
def relevant_ocrs_on_observation_frame_test(dashboard_path):
	this_observer_uuid, _, formalized_uuid_unsplit = dashboard_path.split("/")
	_, formalized_uuid = formalized_uuid_unsplit.split(".")

	# Load in the formalized object and take the first candidate therein (also deriving the frame number as part of the process)
	formalized_obj = load(f"{this_observer_uuid}/formalized/{formalized_uuid}.json")
	data_donation_uuid = formalized_obj[0]["data_donation_uuid"]
	frame_n = formalized_obj[0]["frame"]

	# Run the relevant OCR script
	relevant_ocrs_boxes = relevant_ocrs_on_observation_frame(
		this_observer_uuid, data_donation_uuid, frame_n, load(f"{this_observer_uuid}/temp-v2/{data_donation_uuid}/metadata.json"))

	if (VERBOSE): 
		print("dashboard_path: " + dashboard_path)
		print(json.dumps(relevant_ocrs_boxes, indent=3))
		print("\n\n\n")


# Run for no longer than 14 minutes
# 
N_SECONDS_TIMEOUT = (60 * 14)
def routine_batch(event, context=None):
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_BUCKET_MOBILE_OBSERVATIONS})
	# For each observer, if the entrypoint_cache exists, load in the data donations
	qualified_observer_uuids = list()
	random.shuffle(observer_uuids)
	for _this_observer_uuid in observer_uuids:
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		if (VERBOSE): print(this_observer_uuid)
		entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
		time_at_call = int(time.time())
		if (s3_object_exists(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)):
			routine_instance(this_observer_uuid)
			if (VERBOSE): print("Observer ", this_observer_uuid, " took ", abs(time_at_call - int(time.time())), " seconds")
			elapsed_time = abs(int(time.time()) - time_at_init)
			if (elapsed_time > N_SECONDS_TIMEOUT):
				if (VERBOSE): print("Breaking to avoid overload at batch level...")
				break
	return str()

def routine_instance(this_observer_uuid, N_TO_PROCESS_IN_ONE_INSTANCE=250):
	if (VERBOSE): print(f"Indexing {this_observer_uuid}")
	# Load and order entrypoint_cache so that data can be segmented on a timeline
	entrypoint_cache_path = f"{this_observer_uuid}/entrypoint_cache.json"
	entrypoint_cache = s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)
	entrypoint_cache_ordered = [entrypoint_cache[k] | {"k" : k} for k in entrypoint_cache.keys()]
	entrypoint_cache_ordered = [x for x in entrypoint_cache_ordered if ("observed_at" in x)]
	entrypoint_cache_ordered = sorted(entrypoint_cache_ordered, key=lambda d: d["observed_at"])
	#
	# firstly check the entrypoint cache to determine which data donations:
	# 	1. have ocrs
	# 	2. do not have ccl_terms_identified_v4 on them
	selected_entries = [x["k"] for x in entrypoint_cache_ordered if ((("ocr" in x) and ("failsafe" in x))
														and (not ("ccl_terms_identified_v4" in x)))]

	# TODO - clear the ccl_terms_identified_v2 and 3 entries

	# End early if there is nothing to process
	if (len(selected_entries) == 0):
		return

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
			if (VERBOSE): print("Breaking loop early : N_TO_PROCESS_IN_ONE_INSTANCE")
			break
	expanded_data_donations = sorted(expanded_data_donations, key=lambda d: d["timestamp"])
	expanded_data_donations_dict = {x["data_donation_uuid"]:x for x in expanded_data_donations}
	# evaluate each frame to determine whether its an ad or not - this is done in two ways
	#	1. we identify the sponsored text within the ocr
	#	2. we identify the sponsored term within the yolo
	# we can rely on the failsafe compilation to get an indication on how to proceed here
	unique_data_donation_uuids = list(set([x["data_donation_uuid"] for x in expanded_data_donations]))
	#print(f"{this_observer_uuid}/temp-v2/{expanded_data_donations[0]['data_donation_uuid']}/failsafe.json")
	elapsed_time = int(time.time())
	data_donation_failsafe_cache = dict()
	data_donation_metadata_cache = dict()
	marked_entries = list()
	# Some data donations might be malformed, and so we check them before indexing them, or marking them
	for x in unique_data_donation_uuids:
		try:
			data_donation_failsafe_cache[x] = load(f"{this_observer_uuid}/temp-v2/{x}/failsafe.json")
			data_donation_metadata_cache[x] = load(f"{this_observer_uuid}/temp-v2/{x}/metadata.json")
			marked_entries.append(x)
		except:
			if (VERBOSE): print(f"Excluding malformed entry: {this_observer_uuid}/temp-v2/{x}")
			pass
	elapsed_time = abs(int(time.time()) - elapsed_time)
	if (VERBOSE): print(f"Time taken to index {len(data_donation_failsafe_cache.keys())} entries: ", elapsed_time, " seconds")
	#ipdb.set_trace()
	true_positive_expanded_data_donations = list()
	for x in marked_entries:
		this_entry = expanded_data_donations_dict[x]
		this_platform = data_donation_metadata_cache[x]["nameValuePairs"]["platform"]
		failsafe_obj = data_donation_failsafe_cache[x][this_entry["frame_n"]]
		is_true_positive = (any(failsafe_obj["ocr_frame_sponsored_evaluations"]) 
			or ((failsafe_obj["yolov5_detections"] is not None) and 
				(len(failsafe_obj["yolov5_detections"]) > 0) and
				(max([y["confidence"] for y in failsafe_obj["yolov5_detections"]]) > CONFIDENCE_THRESHOLD[this_platform]))) 
		if (is_true_positive):
			true_positive_expanded_data_donations.append(this_entry)
	'''
		At this stage, we have time-sorted true positives for all data donations

		Then, take each and determine the relevant OCR terms
	'''
	for this_data_donation_frame_obj in true_positive_expanded_data_donations:
		data_donation_uuid = this_data_donation_frame_obj["data_donation_uuid"]
		data_donation_metadata = data_donation_metadata_cache[data_donation_uuid]
		this_frame = this_data_donation_frame_obj["frame_n"]
		# Attach the relevant OCR data
		this_data_donation_frame_obj["relevant_ocrs"] = relevant_ocrs_on_observation_frame(
						this_observer_uuid, data_donation_uuid, this_frame, data_donation_metadata)
		#print(f'Attaching relevant OCR data for {this_observer_uuid} - {this_data_donation_frame_obj["data_donation_uuid"]}')

	# Compile the grouped_terms
	elapsed_time = int(time.time())
	grouped_terms = grouped_terms_from_data_donations(this_observer_uuid, true_positive_expanded_data_donations)
	elapsed_time = abs(int(time.time()) - elapsed_time)
	if (VERBOSE): print(f"Time taken to run groupings:", elapsed_time, " seconds")
	'''
	# Add this entry to the ccl_data_donation_cache (for helping data donations determine which ccl entries they are attributed to)
	ccl_cache = s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_cache.json")
	ccl_data_donation_cache = s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_data_donation_cache.json")
	# Create a UUID to reference the entire group
	this_group_uuid = str(uuid.uuid4())
	for i in range(len(grouped_terms)):
		this_group = grouped_terms[i]
		# For every term in the reweighted_term_splittings...
		this_group_term_uuids = list()
		for this_term in this_group["reweighted_term_splittings"]:
			# Create a UUID to reference the entry within the group
			this_group_term_uuid = str(uuid.uuid4())
			# Add this entry to the ccl_cache (for indexing the progress of the entry)
			ccl_cache[this_group_term_uuid] = {
					"uuid" : this_group_term_uuid,
					"term" : this_term,
					"group_i" : i,
					"platform" : this_group["members"][0]["platform"],
					"ad_type" : this_group["members"][0]["ad_type"],
					"observer_uuid" : this_observer_uuid,
					"group_uuid" : this_group_uuid,
					"timestamp" : min([x["timestamp"] for x in this_group["members"]]) # Take the earliest instance of the time that the query was observed
				}
			this_group_term_uuids.append(this_group_term_uuid)
		for this_member in this_group["members"]:
			ccl_data_donation_cache[f'{this_observer_uuid}/{this_member["data_donation_uuid"]}.json'] = {
					"group_uuid" : this_group_uuid,
					"group_term_uuids" : this_group_term_uuids
				}
	# Write the entire group
	s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, f"grouped_terms/{this_observer_uuid}/{this_group_uuid}.json", grouped_terms)
	
	# Write the caches
	s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_cache.json", ccl_cache)
	s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_data_donation_cache.json", ccl_data_donation_cache)
	'''



	# Add this entry to the ccl_data_donation_cache (for helping data donations determine which ccl entries they are attributed to)
	ccl_cache_appendage = dict()#s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_cache.json")
	#ccl_data_donation_cache = s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_data_donation_cache.json")
	ccl_data_donation_cache = distributed_cache_read({
				"cache" : {
					"bucket" : "fta-mobile-observations-v2-ccl",
					"path" : "ccl_data_donation_cache_distributed"
				},
				"read_keys" : [this_observer_uuid]
			})
	# Create a UUID to reference the entire group
	'''
		Note: Many data donations can be mapped to a single group, but not vice versa
	'''
	this_group_uuid = str(uuid.uuid4())
	for i in range(len(grouped_terms)):
		this_group = grouped_terms[i]
		# For every term in the reweighted_term_splittings...
		this_group_term_uuids = list()
		for this_term in this_group["reweighted_term_splittings"]:
			# Create a UUID to reference the entry within the group
			this_group_term_uuid = str(uuid.uuid4())
			# Add this entry to the ccl_cache (for indexing the progress of the entry)
			ccl_cache_appendage[this_group_term_uuid] = {
					"uuid" : this_group_term_uuid,
					"term" : this_term,
					"group_i" : i,
					"platform" : this_group["members"][0]["platform"],
					"ad_type" : this_group["members"][0]["ad_type"],
					"observer_uuid" : this_observer_uuid,
					"group_uuid" : this_group_uuid,
					"timestamp" : min([x["timestamp"] for x in this_group["members"]]) # Take the earliest instance of the time that the query was observed
				}
			this_group_term_uuids.append(this_group_term_uuid)
		for this_member in this_group["members"]:
			ccl_data_donation_cache[f'{this_observer_uuid}/{this_member["data_donation_uuid"]}.json'] = {
					"observer_uuid" : this_observer_uuid,
					"group_uuid" : this_group_uuid,
					"group_term_uuids" : this_group_term_uuids
				}
	# Write the entire group
	s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, f"grouped_terms/{this_observer_uuid}/{this_group_uuid}.json", grouped_terms)
	
	# Write the caches
	#s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_cache.json", ccl_cache)
	distributed_cache_write({
				"cache" : {
					"bucket" : S3_BUCKET_MOBILE_OBSERVATIONS_CCL,
					"path" : "ccl_cache_distributed"
				},
				"longitudinal_unit" : A_DAY,
				"longitudinal_key" : ["timestamp"],
				"input" : ccl_cache_appendage
			})


	#s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_data_donation_cache.json", ccl_data_donation_cache)
	distributed_cache_write({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_data_donation_cache_distributed"
			},
			"categorical" : True,
			"longitudinal_key" : ["observer_uuid"],
			"input" : ccl_data_donation_cache
		})



	# Finally, mark the data donations in the entrypoint_cache
	entrypoint_cache = s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path)
	for indexed_entry in marked_entries:
		entrypoint_cache[indexed_entry]["ccl_terms_identified_v4"] = {"at" : int(time.time())}
	s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS, entrypoint_cache_path, entrypoint_cache)
	# The formalized cache for this entry must be retrieved - TODO - not sure what this is for...

	#with open("true_positive_expanded_data_donations.json", "w") as f: f.write(json.dumps(true_positive_expanded_data_donations,indent=3))
	#with open("test_true_positive_expanded_data_donations.json", "w") as f: f.write(json.dumps(true_positive_expanded_data_donations,indent=3))
	#true_positive_expanded_data_donations = json.loads(open("test_true_positive_expanded_data_donations.json").read())

	'''
		create a ccl identity that maps back to a collection of data donations
			the ccl identity consists of:
				* the raw candidates of text
				* the 'best candidate' terms
				TODO - take a sample of data and see how often it returns the correct data
	# TODO - we need to observe the forming of a correct candidate in realtime across various frames 
		mark the data donations to indicate that they've been processed
	'''
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
	'''


processes = { "batch" : routine_batch }

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

def repair_routine():
	# Load in the ccl_cache
	ccl_cache = distributed_cache_read({
			"cache" : {
				"bucket" : "fta-mobile-observations-v2-ccl",
				"path" : "ccl_cache_distributed"
			}
		})
	# Find the related details
	group_cache = dict()
	i = int()
	for k in ccl_cache:
		i += 1
		if (VERBOSE): print(i)
		group_path = f'grouped_terms/{ccl_cache[k]["observer_uuid"]}/{ccl_cache[k]["group_uuid"]}.json'
		if (not ccl_cache[k]["group_uuid"] in group_cache):
			group_cache[ccl_cache[k]["group_uuid"]] = s3_dict_read(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, group_path)
		this_group = group_cache[ccl_cache[k]["group_uuid"]]
		ccl_cache[k] = ccl_cache[k] | {
				"platform" : this_group[ccl_cache[k]["group_i"]]["members"][0]["platform"],
				"ad_type" : this_group[ccl_cache[k]["group_i"]]["members"][0]["ad_type"]
			}
	# Put it back
	#s3_dict_write(S3_BUCKET_MOBILE_OBSERVATIONS_CCL, "ccl_cache.json", ccl_cache)
	distributed_cache_write({
				"cache" : {
					"bucket" : S3_BUCKET_MOBILE_OBSERVATIONS_CCL,
					"path" : "ccl_cache_distributed"
				},
				"longitudinal_unit" : A_DAY,
				"longitudinal_key" : ["timestamp"],
				"input" : ccl_cache
			})

if (__name__ == "__main__"):
	pass
	routine_instance("OBSERVER_UUID_GOES_HERE")
	routine_batch(None)
	#pass
	#sliding_levenshtein_pct_annotated("afrog.com", "froa.com")
	#best_candidate_terms("template_observer_uuid", json.loads(open("true_positive_expanded_data_donations.json").read()))
	#routine_batch(None)
	#repair_routine()





