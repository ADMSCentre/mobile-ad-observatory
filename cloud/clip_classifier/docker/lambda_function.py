
import os
import re
import sys
import json
import time
import math
import torch
import random
import open_clip
import numpy as np
from PIL import Image
from collections import Counter
from safetensors.numpy import save_file, load_file
import traceback
import boto3
import botocore
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError
from io import BytesIO
import boto3
from botocore.exceptions import ClientError
from PIL import Image, UnidentifiedImageError

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
S3_OBSERVATIONS_BUCKET = "fta-mobile-observations-v2"
S3_MOBILE_OBSERVATIONS_CCL_BUCKET = "fta-mobile-observations-v2-ccl"
AWS_REQUIRED_RESOURCES = ["s3"]

def safeget(data, keys):
	current = data
	for key in keys:
		if isinstance(current, dict) and key in current:
			current = current[key]
		else:
			return None
	return current

def normalize_spaces(text): return re.sub(r'\s+', ' ', text.strip())


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

def s3_img_to_pil(bucket, key):
	try:
		try: data = AWS_CLIENT["s3"].get_object(**{"Bucket": bucket, "Key": key})["Body"].read()
		except ClientError as e: return None
		try: return Image.open(BytesIO(data))
		except UnidentifiedImageError: return None
	except:
		return None

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

MODEL_CONFIG = {
	"alpha" : 0.8,
	"text_weight" : 0.8,
	"image_weight" : 0.2,
	"k" : 5,
	"pct_precision" : 3,
	"name" : "ViT-B-32",
	"device" : "cpu",
	"pretrained_docker" : "/opt/models/CLIP-ViT-B-32-laion2b_s34b_b79k/open_clip_pytorch_model.bin",
	"pretrained_direct" : "laion2b_s34b_b79k"
}
MODEL, preprocess, _ = open_clip.create_model_and_transforms(MODEL_CONFIG["name"], 
	pretrained=MODEL_CONFIG[f"pretrained_{('direct' if (__name__ == "__main__") else 'docker')}"])
MODEL = MODEL.to(MODEL_CONFIG["device"]).eval()
STORED_SAFETENSORS = load_file("stored.safetensors")
STORED_JSON = json.loads(open("stored.json").read())
IDX2CAT = {i: cat for i, cat in enumerate(STORED_JSON["raw_categories"])}
CAT2IDX = {cat: idx for idx, cat in enumerate(STORED_JSON["raw_categories"])}
with torch.no_grad():
	CAT_EMBEDS = MODEL.encode_text(open_clip.tokenize(STORED_JSON["categories"]).to(MODEL_CONFIG["device"]))
	CAT_EMBEDS = (CAT_EMBEDS / CAT_EMBEDS.norm(dim=-1, keepdim=True)).cpu().numpy()

if (__name__ == "__main__"):
	import ipdb

'''
	This function natively calculates the cosine similarity of two vectors
'''
def cosine_similarity(X, Y=None, dense_output=True):
	X = np.atleast_2d(np.asarray(X, dtype=float))
	Y = X if Y is None else np.atleast_2d(np.asarray(Y, dtype=float))
	X_norm = np.linalg.norm(X, axis=1, keepdims=True)
	Y_norm = np.linalg.norm(Y, axis=1, keepdims=True)
	X_norm[X_norm == 0] = 1e-15
	Y_norm[Y_norm == 0] = 1e-15
	sim = np.dot(X, Y.T) / (X_norm @ Y_norm.T)
	return np.asarray(sim) if dense_output else sim

'''
	This function accepts a text and image candidate, yielding the top 5 rankings
'''
def classifier(candidate):
	elapsed_time = time.time()
	with torch.no_grad(): image_embedding = MODEL.encode_image(preprocess(candidate["img"].convert("RGB")).unsqueeze(0).to(MODEL_CONFIG["device"])).cpu().numpy()
	with torch.no_grad(): text_embedding = MODEL.encode_text(open_clip.tokenize([candidate["text"]]).to(MODEL_CONFIG["device"])).cpu().numpy()
	# Embedding fusion
	support_embeddings = ((MODEL_CONFIG["text_weight"] * STORED_SAFETENSORS["text_emb_support"]) 
								+ (MODEL_CONFIG["image_weight"] * STORED_SAFETENSORS["image_emb_support"]))
	chunk_embeddings = MODEL_CONFIG["text_weight"] * text_embedding + MODEL_CONFIG["image_weight"] * image_embedding
	# Similarities
	similarities_fewshot = cosine_similarity(chunk_embeddings, support_embeddings)
	similarities_zeroshot = cosine_similarity(text_embedding, CAT_EMBEDS)
	# Few-shot voting
	fewshot_scores = np.zeros((len([1]), len(STORED_JSON["raw_categories"]))) # 1 entry gives 1 length
	top_k_indices = np.argsort(similarities_fewshot, axis=1)[:, -MODEL_CONFIG["k"]:]
	for q_idx, neighbors in enumerate(top_k_indices):
		neighbor_labels = [STORED_JSON["support_labels"][i] for i in neighbors]
		counter = Counter(neighbor_labels)
		for label, count in counter.items():
			fewshot_scores[q_idx, CAT2IDX[label]] = count
	fewshot_scores /= MODEL_CONFIG["k"]
	# Combine + predict
	combined_scores = MODEL_CONFIG["alpha"] * similarities_zeroshot + (1 - MODEL_CONFIG["alpha"]) * fewshot_scores
	top5_indices = np.argsort(combined_scores, axis=1)[:, -MODEL_CONFIG["k"]:][:, ::-1]
	return {
		"elapsed_time" : abs(elapsed_time - time.time()),
		"classification" : [{
				"ranking" : rank,
				"label" : IDX2CAT[top5_indices[0][rank]],
				"score" : float([round(combined_scores[i, idx], MODEL_CONFIG["pct_precision"]) 
														for i, idx in enumerate(top5_indices[:, rank])][0])
		} for rank in range(MODEL_CONFIG["k"])]}

def meta_adlibrary_mass_download_resource(mass_download_result, this_ccl_uuid, this_url):
	if (not "outlinks" in mass_download_result):
		return None
	candidates = [x for x in mass_download_result["outlinks"].values() if (x["url"] == this_url)]
	if (len(candidates) == 0):
		return None
	else:
		resource_key_trail = f'{candidates[0]["outlink_uuid"]}.{candidates[0]["content_type"]}'
		return f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_ccl_uuid}/mass_download/{resource_key_trail}'

def routine_instance(event, context):
	'''
		Find the unsplit RDO UUID

		by going to the quick access cache and identifying the entry attached to the formaliezd uuid
	'''
	elapsed_time = time.time()
	quick_access_cache = s3_cache_read(S3_OBSERVATIONS_BUCKET, f"{event['observer_uuid']}/quick_access_cache.json")
	rdo_uuid_unsplit = None
	for k in quick_access_cache["ads_passed_rdo_construction"]:
		if (k.endswith(event['rdo_uuid']+"/")):
			_, _, rdo_uuid_unsplit, _ = k.split("/")
			break

	tentative_rdo_path = f"{event['observer_uuid']}/rdo/{rdo_uuid_unsplit}/output.json"
	if (rdo_uuid_unsplit is None) or (not s3_object_exists(S3_OBSERVATIONS_BUCKET, tentative_rdo_path)):
		print("No RDO found for: ")
		print(json.dumps(event))
		return str()

	this_rdo_obj = s3_cache_read(S3_OBSERVATIONS_BUCKET, tentative_rdo_path)

	OCR_CUTOFF_THRESHOLD = 0.75

	classification_candidates = list()

	# Attach candidates relating to the observation itself
	for this_keyframe in  this_rdo_obj["observation"]["keyframes"]:
		classification_candidates.append({
				"img_path" : { "key" : this_keyframe["screenshot_cropped"], "bucket" : S3_OBSERVATIONS_BUCKET },
				"text_raw" : normalize_spaces(" ".join([x["text"] for x in this_keyframe["ocr_data"] if (x["confidence"] >= OCR_CUTOFF_THRESHOLD)])),
				"source" : "observation"
			})

	# Attach candidates relating to the ccl if possible
	if ("ccl_v2" in this_rdo_obj["enrichment"]):
		for this_ccl_uuid in this_rdo_obj["enrichment"]["ccl_v2"]:
			# Vendor-specific
			if (this_rdo_obj["enrichment"]["ccl_v2"][this_ccl_uuid]["vendor"] == "META_ADLIBRARY"):

				# Load in the mass download result
				mass_download_result = s3_cache_read(S3_MOBILE_OBSERVATIONS_CCL_BUCKET, 
					f'outputs/meta_adlibrary/meta_adlibrary_scrapes/{this_ccl_uuid}/mass_download/mass_download_result.json')

				# For each attachment
				tentative_json_interpreted = safeget(this_rdo_obj["enrichment"]["ccl_v2"][this_ccl_uuid], ["scrape_output", "response_interpreted", "json_interpreted"])
				if (tentative_json_interpreted is not None):
					for i in range(len(tentative_json_interpreted)):
						this_ccl_attachment = tentative_json_interpreted[i]
						# An image may not always be available - fall back onto the profile picture of the page
						image_fallback_url = safeget(this_ccl_attachment, ["snapshot", "page_profile_picture_url"])
						image_fallback_resource_key = (None if (image_fallback_url is None) else 
														meta_adlibrary_mass_download_resource(mass_download_result, this_ccl_uuid, image_fallback_url))

						# Determine the super-text
						aggregate_super_text = str()
						for k in ["byline", "disclaimer_label", "page_name", "caption", "cta_text", "link_description", "title"]:
							tentative_val = safeget(this_ccl_attachment, ["snapshot", k])
							if (type(tentative_val) is str):
								aggregate_super_text += (" " + tentative_val)

						# Attach body text to super-text
						tentative_body_text = safeget(this_ccl_attachment, ["snapshot", "body", "text"])
						if (type(tentative_body_text) is str):
							aggregate_super_text += (" " + tentative_body_text)
						
						# Attach page categories to super-text
						tentative_page_categories = safeget(this_ccl_attachment, ["snapshot", "page_categories"])
						if (type(tentative_page_categories) is list):
							for k in tentative_page_categories:
								aggregate_super_text += f" {k}"

						# Evaluate cards (if they exist)
						tentative_ccl_attachment_cards = safeget(this_ccl_attachment, ["snapshot", "cards"])
						if ((type(tentative_ccl_attachment_cards) is list) and (len(tentative_ccl_attachment_cards) > 0)):
							for j in range(len(tentative_ccl_attachment_cards)):
								this_card = tentative_ccl_attachment_cards[j]
								# Determine the text
								aggregate_text = str()
								for k in ["body", "caption", "link_description", "title", "cta_text"]:
									tentative_val = safeget(this_card, [k])
									if (type(tentative_val) is str):
										aggregate_text += (" " + tentative_val)

								# Determine the image
								this_determined_img_key = image_fallback_resource_key
								tentative_image_url = safeget(this_card, ["original_image_url"])
								if (type(tentative_image_url) is str):
									tentative_image_url_key = meta_adlibrary_mass_download_resource(mass_download_result, this_ccl_uuid, tentative_image_url)
									if (tentative_image_url_key is not None):
										this_determined_img_key = tentative_image_url_key

								classification_candidates.append({
										"img_path" : { "key" : this_determined_img_key, "bucket" : S3_MOBILE_OBSERVATIONS_CCL_BUCKET },
										"text_raw" : normalize_spaces(aggregate_super_text + " " + aggregate_text),
										"source" : "ccl",
										"source_detail" : {
											"ccl_uuid" : this_ccl_uuid,
											"ccl_attachment_i" : i,
											"ccl_attachment_card_i" : j
										}
									})
						else:
							# Determine the image super-candidate
							this_determined_img_key = image_fallback_resource_key
							tentative_snapshot_images = safeget(this_ccl_attachment, ["snapshot", "images"])
							if (type(tentative_snapshot_images) is list):
								for image_obj in tentative_snapshot_images:
									# We haven't yet observed a case where there is an image to know what key it provides, and so we are
									# just going to get the URL with the longest length
									image_obj_url = max([x for x in image_obj.values() if (not x is None)], key=len)
									tentative_image_obj_url_key = meta_adlibrary_mass_download_resource(mass_download_result, this_ccl_uuid, image_obj_url)
									if (type(tentative_image_obj_url_key) is str):
										this_determined_img_key = tentative_image_obj_url_key
										break
							classification_candidates.append({
									"img_path" : { "key" : this_determined_img_key, "bucket" : S3_MOBILE_OBSERVATIONS_CCL_BUCKET },
									"text_raw" : normalize_spaces(aggregate_super_text),
									"source" : "ccl",
									"source_detail" : {
										"ccl_uuid" : this_ccl_uuid,
										"ccl_attachment_i" : i
									}
								})

	composite_candidate = dict()

	# For each of the retrieved candidates, run the classification
	for this_candidate in classification_candidates:
		classification_outcome = dict()
		if ((this_candidate["img_path"] is not None) and (this_candidate["text_raw"] is not None) 
										and (len(normalize_spaces(this_candidate["text_raw"])) > 0)):
			this_pil_img = s3_img_to_pil(this_candidate["img_path"]["bucket"] , this_candidate["img_path"]["key"])
			if (not this_pil_img is None):
				classification_outcome["status"] = "SUCCESS"
				classification_outcome["content"] = classifier({
						"img" : this_pil_img,
						"text" : this_candidate["text_raw"]
					})
			else:
				classification_outcome["status"] = "ERROR"
				classification_outcome["comment"] = "MISSING_IMAGE"
		else:
			classification_outcome["status"] = "ERROR"
			classification_outcome["comment"] = "MALFORMED_MEDIA"

		this_candidate |= classification_outcome

		# Assist in constructing the composite
		if ("content" in classification_outcome):
			for this_classification in classification_outcome["content"]["classification"]:
				if (not this_classification["label"] in composite_candidate):
					composite_candidate[this_classification["label"]] = float()
				composite_candidate[this_classification["label"]] += this_classification["score"]

	composite_classification_raw = {k: v for k, v in sorted(composite_candidate.items(), key=lambda x: x[1], reverse=True)}
	ranking_i = int(); composite_classification = list()
	for k,v in composite_classification_raw.items():
		composite_classification.append({
				"ranking" : ranking_i,
				"label" : k,
				"score_normalized" : (v / len(classification_candidates))
			})
		ranking_i += 1

	output = {
		"classified_at" : int(time.time()),
		"elapsed_time" : abs(elapsed_time - time.time()),
		"candidates" : classification_candidates,
		"composite_classification" : composite_classification
	}

	if (__name__ == "__main__"):
		print(json.dumps(output,indent=3))
		ipdb.set_trace()
	# Write the result
	s3_cache_write(S3_OBSERVATIONS_BUCKET, f'{event['observer_uuid']}/clip_classifications/{event["rdo_uuid"]}.json', output)

	# Reflect the result in the formalized cache
	formalized_cache = s3_cache_read(S3_OBSERVATIONS_BUCKET, f"{event['observer_uuid']}/formalized_cache.json")
	formalized_cache[event["rdo_uuid"]]["clip_classification"] = int(time.time())
	s3_cache_write(S3_OBSERVATIONS_BUCKET, f"{event['observer_uuid']}/formalized_cache.json", formalized_cache)

	return {
		'statusCode': 200,
		'body': json.dumps(dict())
	}

N_SECONDS_TIMEOUT = (60 * 14) + 30
def routine_batch(event, context=None):
	classifier_intents = list()
	time_at_init = int(time.time())
	# Get all observer UUIDs
	observer_uuids = subbucket_contents({"Bucket" : S3_OBSERVATIONS_BUCKET})

	# For each observer, if the entrypoint_cache exists, load in the data donations
	n_done = int()
	n_not_done = int()
	qualified_observer_uuids = list()
	for _this_observer_uuid in observer_uuids: # observer_uuids
		this_observer_uuid = _this_observer_uuid.replace("/",str())
		if (__name__ == "__main__"):
			print(this_observer_uuid)
		formalized_cache_path = f"{this_observer_uuid}/formalized_cache.json"
		if (s3_object_exists(S3_OBSERVATIONS_BUCKET, formalized_cache_path)):
			formalized_cache = s3_cache_read(S3_OBSERVATIONS_BUCKET, formalized_cache_path)
			for formalized_uuid in formalized_cache:
				if (not "clip_classification" in formalized_cache[formalized_uuid]):
					classifier_intents.append({
							"observer_uuid" : this_observer_uuid,
							"rdo_uuid" : formalized_uuid
						})
					n_not_done += 1
				else:
					n_done += 1
				#if (len(classifier_intents) > XXX):
				#	break
		#if (len(classifier_intents) > XXX):
		#	break
	# Shuffle the RDOs that need to be executed
	random.shuffle(classifier_intents)
	if (__name__ == "__main__"):
		ipdb.set_trace()
	# Execute with early timeout if necessary
	for x in classifier_intents:
		time_at_call = int(time.time())
		print("\tExecuting observer_uuid: ", x["observer_uuid"], " rdo_uuid: ", x["rdo_uuid"])
		routine_instance(x, None)
		print("\t\tElapsed time: ", abs(time_at_call - int(time.time())), " seconds")
		elapsed_time = abs(int(time.time()) - time_at_init)

		if ((__name__ != "__main__") and (elapsed_time > N_SECONDS_TIMEOUT)):
			break
	return str()

processes = { 
		"routine_instance" : routine_instance,
		"routine_batch" : routine_batch
	}

def lambda_handler(event, context):
	response_body = dict()
	if ("action" in event):
		if (event["action"] in processes):
			print("Action: ", event["action"])
			response_body = processes[event["action"]](event, context)
	return { 'statusCode': 200, 'body': json.dumps(response_body) }

if (__name__ == "__main__"):
	routine_batch(None, None)
	'''
	lambda_handler({
			"action" : "routine_instance",
			"observer_uuid" : "51a3ddb4-38eb-4c43-aee4-3c37628eb6f5",
			"rdo_uuid" : "1ab2c557-5c59-43e2-b7e5-9a43804cf922"
		}, None)
	'''




