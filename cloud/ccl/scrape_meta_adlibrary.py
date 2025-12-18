'''
	
	needs to facilitate management of identities used to scrape meta

	needs to also include functionality for safely scraping meta


	it is the scrape_meta_adlibrary.py's job to allocate a scrape identity and set the requirements
	for its creation

'''

import os
import sys
import json
import time
from pathlib import Path
import boto3
import requests
import urllib
import shutil
import re
import uuid
import mergedeep
import smtplib
import ssl
from email.message import EmailMessage
import traceback
from sliding_levenshtein import *
from scrape import *

VERBOSE = True

TEST_IS_DIAGNOSTIC = ((len(sys.argv) == 4) and (sys.argv[3] == "diagnostic"))
TEST_KEYWORD = "cola"
if ((__name__ == "__main__") and (sys.argv[1] == "cache")):
	if ((len(sys.argv) == 4) and (sys.argv[3] != "diagnostic")):
		TEST_KEYWORD = sys.argv[3]
	else:
		TEST_KEYWORD = input("Supply a test keyword: ")

close_and_reopen_window_str = '"errorDescription":"Please try closing and re-opening your browser window."'
missing_variable_error_str = 'A server error missing_required_variable_value occured.'

if (__name__ == "__main__"):
	import ipdb
AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

'''
	Load JSON from the AWS 'holding' bucket
'''
def json_s3_load_holding(this_key):
	try:
		response = AWS_CLIENT["s3"].get_object(Bucket=HOLDING_BUCKET, Key=this_key)
		return json.loads(response['Body'].read().decode("utf-8"))
	except:
		if (VERBOSE): print(str(traceback.format_exc()))
		return dict()

'''
	Save JSON to the AWS 'holding' bucket
'''
def json_s3_save_holding(this_key, this_data):
	AWS_RESOURCE["s3"].Object(HOLDING_BUCKET, this_key).put(Body=json.dumps(this_data, indent=3))

'''
	Delete JSON
'''
def json_s3_delete_holding(this_key):
	AWS_RESOURCE["s3"].Object(HOLDING_BUCKET, this_key).delete()

##############################################################################################################################
##############################################################################################################################
### Utilities
##############################################################################################################################
##############################################################################################################################
'''
MAX_LEVENSHTEIN_DISTANCE = 5
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
'''
##############################################################################################################################
##############################################################################################################################
### SCRAPER FUNCTIONALITY
##############################################################################################################################
##############################################################################################################################


'''
	This function prepares common details held in all Meta Facebook Ad Library 'request' designs
'''
def meta_adlibrary_prepare_request_common(request_config, request_template):
	this_doc_id = None
	# Overrides of the doc ID are necessary during cacheing of data
	if ("doc_id_override" in request_config):
		this_doc_id = request_config["doc_id_override"]
	else:
		this_doc_id = request_template["html_variables"]["doc_id"]
	preparation = {
		"include" : {
			"cookies" : {},
			"headers" : {
				"referer" : f"https://www.facebook.com/ads/library/?{urllib.parse.urlencode(request_config['ad_capture_appendage'])}",
				'x-fb-friendly-name': request_config["type"],
				'x-fb-lsd': request_template["html_variables"]["x-fb-lsd"],
				#"x-asbd-id" : '129477'
			},
			"data" : {
				#'__s': 'pq2kcn:u1fdva:ldokiy',
				#'__dyn': '7xe6EiwgUCdwn8K2Wmh0MBwCwpUnwgU6C7UW3K4EowNwcy2q0_EtxG4o0B-qbwgE7R046xO2O1VwBwXwEwgo9o1eE4a4oaEd86a3a0EA2C0hi5E6i588Egz898mwoHwrUcUjwGzE2VKU28xaaws8nwhE34yE16Ec8-3qazo8U3ywbLwrU6Ci2G0z85C1Iwqo1uo7u1rw',
				#'__csr': 'iiROtsLkKmKeBAzuECfV8-y1aqcRgEBfDlfn-ABxuHHXBRw8m14wqovwyxi26q0L898rw6gw58w1hy008qcw',
				'fb_api_req_friendly_name': request_config["type"],
				'variables': json.dumps(request_config["variables"]),
				'doc_id': this_doc_id
			}
		}
	}
	return preparation

'''
	This function prepares a request for send-off, assembling the headers, data, cookies, etc.
'''
def meta_adlibrary_prepare_request(_request_config, this_scrape_identity, country_override=None):
	request_config = dict(_request_config)
	if (country_override is not None):
		request_config["ad_capture_appendage"]["country"] = country_override
	if (request_config["type"] == "AdLibraryAddSavedSearchModalPagesQuery"):
		request_config["variables"]["sessionId"] = this_scrape_identity["request_template"]["html_variables"]["session_id"]

	if (request_config["type"] == "AdLibraryMobileFocusedStateProviderRefetchQuery"):
		request_config["variables"]["sessionID"] = this_scrape_identity["request_template"]["html_variables"]["session_id"]
	this_request_preparation = meta_adlibrary_prepare_request_common(request_config, this_scrape_identity["request_template"])

	# Combine template with inclusions
	relevant_keys = ["cookies", "headers", "data"]
	applied = dict()
	for k in relevant_keys:
		if (k in this_scrape_identity["request_template"]):
			# Using values directly from the request template
			applied[k] = dict(this_scrape_identity["request_template"][k])
		if (k in this_request_preparation["include"]):
			if (k in this_scrape_identity["request_template"]):
				# Combining values
				applied[k] = mergedeep.merge(applied[k], this_request_preparation["include"][k])
			else:
				# Using values directly from the request preparation
				applied[k] = dict(this_request_preparation["include"][k])
	# Make exclusions
	if ("exclude" in this_request_preparation):
		for relevant_key in this_request_preparation["exclude"]:
			for field in this_request_preparation["exclude"][relevant_key]:
				if ((relevant_key in applied) and (field in applied[relevant_key])):
					del applied[relevant_key][field]
	return applied


'''
	This function sends a request to Meta Facebook
'''
def meta_adlibrary_request_send(this_request_config, this_scrape_identity, injected_request_config_base=dict(), country_override=None):
	if (country_override is not None):
		this_request_config["variables"]["country"] = country_override
		this_request_config["variables"]["countries"] = [country_override]
	# Prepare the request parameters
	request_config_base = mergedeep.merge(GLOBALS_CONFIG["meta_adlibrary"]["request_config_base"], injected_request_config_base)
	this_request_preparation = meta_adlibrary_prepare_request(
		mergedeep.merge(request_config_base, this_request_config), this_scrape_identity, country_override=country_override)
	# Send the request
	response = requests.post("https://www.facebook.com/api/graphql/", 
			proxies=this_scrape_identity["proxies"]["meta_adlibrary"], # We have to deliberately set this for Meta Ad Library
			cookies=this_request_preparation["cookies"],
			headers=this_request_preparation["headers"],
			data=this_request_preparation["data"]
		)
	return response






'''
	This process provisions a proxy for set up of a scraper identity - it instantiates the scraper
	within the online holding bucket, and awaits 
'''
def provision_for_scrape_identity():
	# Construct proxy strings for the Meta Ad Library scraper
	this_public_ip_address_meta_adlibrary = sys.argv[2]
	proxies = {"meta_adlibrary" : this_public_ip_address_meta_adlibrary, "mass_downloads" : sys.argv[3]}
	general_proxy_config = json.loads(open(os.path.join(LOCAL_TINYPROXY_PATH, "configs", "ec2_instances.json")).read())
	for this_alt in proxies:
		proxies[this_alt]
		this_public_ip_address = proxies[this_alt]
		# Run a check on the proxy's existence (as an EC2 instance) before provisioning it to the scrape identity
		this_proxy_obj = retrieve_proxy_for_ip_address(this_public_ip_address, LOCAL_TINYPROXY_PATH)
		if (this_proxy_obj is None):
			if (VERBOSE): print("Proxy error")
			raise Exception()
		else:
			general_proxy_config["public_ip_address"] = this_public_ip_address
			proxies[this_alt] = assemble_proxy_string(general_proxy_config)
	# Assert that there is a Selenium driver in place for the 'Meta AdLibrary' proxy
	selenium_driver_string = f"selenium_driver_{this_public_ip_address_meta_adlibrary.replace('.','_')}_new" # TODO - correct this
	if (not os.path.exists(os.path.join(LOCAL_TINYPROXY_PATH, selenium_driver_string))):
		if (VERBOSE): print("Selenium driver does not exist for IP address: ", this_public_ip_address_meta_adlibrary)
		raise Exception()
	this_uuid = str(uuid.uuid4())
	this_scrape_identity = {
			"uuid" : this_uuid,
			"vendor" : "META_ADLIBRARY",
			"valid" : False,
			"last_cached_at" : None,
			"public_ip_address_meta_adlibrary" : this_public_ip_address_meta_adlibrary,
			"selenium_driver_string" : selenium_driver_string,
			"request_template" : None,
			"created_at" : int(time.time()),
			"proxies" : proxies,
			"scrape_log" : list(),
		}
	json_s3_save_holding(f"scrape_identities/{this_uuid}.json", this_scrape_identity)
	if (VERBOSE): print(json.dumps(this_scrape_identity,indent=3))
	ipdb.set_trace()
	pass


'''
	This function extracts the cookies from the Meta Facebook website
'''
def meta_adlibrary_extract_base_cookies(driver):
	DESIRED_COOKIES = ["ps_l", "ps_n", "sb", "datr", "locale", "c_user", "wd", "xs", "fr", "presence"]
	driver.get("https://facebook.com/")
	time.sleep(5)
	output_cookies = dict()
	for this_cookie in driver.get_cookies():
		if (this_cookie["name"] in DESIRED_COOKIES):
			output_cookies[this_cookie["name"]] = this_cookie["value"]
	return output_cookies

'''
	This function extracts miscellaneous HTML variables, which are of use to Meta Facebook Ad Library 'request' designs
'''
def meta_adlibrary_extract_misc_html_variables(driver, proxies):
	ad_capture_appendage = GLOBALS_CONFIG["meta_adlibrary"]["ad_capture_appendage"]
	regex_patterns = {
			"x-fb-lsd" : r"(?<=\"LSD\"\,\[\]\,\{\"token\"\:\").*?(?=\")",
			"__hsi" : r'(?<=\"e\"\:\").*?(?=\")',
			"fb_dtsg" : r'(?<=\"f\"\:\").*?(?=\")',
			"jazoest" : r'(?<=jazoest\=).*?(?=\")',
			"__rev" : r'(?<=\{\"rev\"\:).*?(?=\})', # doubles up as "__spin_r"
			"__spin_b" : r'(?<=\"\_\_spin\_b\"\:\").*?(?=\")',
			"__spin_t" : r'(?<=\"\_\_spin\_t\"\:).*?(?=\,)',
			"__a" : r'(?<=\?\_\_a\=).*?(?=\&)',
			"__ccg" : r'(?<=\"connectionClass\"\:\").*?(?=\")',
			"__aaid" : r'(?<=\"\_\_aaid\"\:\").*?(?=\")',
			"__hs" : r'(?<=\"haste\_session\"\:\").*?(?=\")',
			"__comet_req" : r'(?<=\_\_comet\_req\=).*?(?=\&)',
			"session_id" : r'(?<=\,\"sessionId\"\:\").*?(?=\")'
		}
	driver.get(f"https://www.facebook.com/ads/library/?{urllib.parse.urlencode(ad_capture_appendage)}")

	# Retrieve all Javscript links within the top of the page's HTML - then request the code of each, and
	# check for mention of the AdLibraryMobileFocusedStateProviderRefetchQuery - the doc_id sits within
	# adjacency of this detail, and only occurs in a few of the total scripts
	time.sleep(5)
	this_page_outer_html = driver.execute_script("return document.documentElement.outerHTML")
	'''
	ipdb.set_trace()
	http_parameters = dict()
	for this_response in driver.requests:
		if (this_response.response):
			# While we can also parse the cookies and query string, we've found
			# that these are arbitrarily referenced, depending on the request format
			http_parameters[this_response.url] = {
					"request_headers" : dict(this_response.headers),
					"response_headers" : dict(this_response.response.headers)
				}
			try: http_parameters[this_response.url]["response_body"] = this_response.response.body.decode("utf-8")
			except: pass

			# https://static.xx.fbcdn.net/rsrc.php/v4iSWu4/yZ/l/en_GB-j/623PYj8dD5i.js 25292167653719228


	with open("dump2.json", "w") as f: f.write(json.dumps(http_parameters,indent=3))
	ipdb.set_trace()
	'''
	if (VERBOSE): print("Examining doc IDs")
	doc_id_targets = {x:list() for x in ["search", "typeahead"]}
	doc_id_targets_prescriptions = {
			"search" : ["AdLibrarySearchPaginationQuery_facebookRelayOperation", "AdLibraryMobileFocusedStateProviderRefetchQuery_facebookRelayOperation"],
			"typeahead" : ["AdLibraryTypeaheadSuggestionDataSourceQuery_facebookRelayOperation"]
		}
	try:
		for this_target in doc_id_targets_prescriptions:
			doc_id_potential_lead_links = [x for x in re.findall(
				r'(?<=\<link rel\=\"preload\" href\=\").*?(?=\")', this_page_outer_html, re.MULTILINE | re.DOTALL) if ("static.xx" in x)]
			for potential_lead_on_doc_id in doc_id_potential_lead_links:
				try:
					# Get the response
					response_html = requests.get(potential_lead_on_doc_id, proxies=proxies, timeout=3).content.decode("utf-8")
					for this_alt in doc_id_targets_prescriptions[this_target]:
						try: 
							for this_lead in re.findall(r'(?<='+ this_alt + r').*?(?=\})', response_html):
								doc_id_targets[this_target].extend(re.findall(r'(?<=")[0-9]{13,17}(?=")', this_lead))
						except: 
							if (VERBOSE): print(traceback.format_exc())
					if (False):
						for this_alt in ["AdLibrarySearchPaginationQuery_facebookRelayOperation", "AdLibraryMobileFocusedStateProviderRefetchQuery_facebookRelayOperation"]:
							try: doc_id_targets.extend(re.findall(r'(?<=' + this_alt + r'"\,\[\]\,\(function\(a,b,c,d,e,f\)\{e\.exports\=").*?(?=")', response_html))
							except: pass
					if (False):
						if ("AdLibraryMobileFocusedStateProviderRefetchQuery" in response_html):
							doc_id_target = re.findall(
								r'(?<=params\:\{id).*?(?=AdLibraryMobileFocusedStateProviderRefetchQuery)', response_html)[-1]
							doc_id_target = re.findall(r'[0-9]+', doc_id_target)[-1]
							doc_id_targets.append(doc_id_target)
						else:
							if (VERBOSE): print("Could not find doc ID using method A, trying method B")
							ipdb.set_trace()
							'AdLibraryPageHoverCardQuery_facebookRelayOperation",[],(function(a,b,c,d,e,f){e.exports=' in response_html
							try:
								doc_id_targets.extend(re.findall(
									r'(?<=")[0-9]{17}(?=")', response_html))
							except: 
								if (VERBOSE): print("Method B failed")
				except: 
					if (VERBOSE): print(traceback.format_exc())
	except:
		if (VERBOSE): print(traceback.format_exc())
	# Continue on with the typical regex extraction of all other fields on the page's HTML
	regex_outputs = dict()
	regex_outputs["tentative_doc_ids"] = list(set(doc_id_targets["search"]))
	regex_outputs["tentative_doc_ids_typeahead"] = list(set(doc_id_targets["typeahead"]))
	# ipdb.set_trace()
	for k in regex_patterns:
		try:
			regex_outputs[k] = re.findall(regex_patterns[k], this_page_outer_html)[0]
		except:
			regex_outputs[k] = None
	return regex_outputs

'''
	This function extracts content for the 'data' parameter of Meta Facebook Ad Library 'request' designs, from the
	supplied request_template
'''
def meta_adlibrary_extract_base_data(request_template):
	if (not "c_user" in request_template["cookies"]):
		request_template["cookies"]["c_user"] = "0"
	return {
			'av': request_template["cookies"]["c_user"],
			'__aaid': request_template["html_variables"]["__aaid"],
			'__user': request_template["cookies"]["c_user"],
			'__a': request_template["html_variables"]["__a"],
			'__req': '1',
			'__hs': request_template["html_variables"]["__hs"],
			'dpr': '2',
			'__ccg': request_template["html_variables"]["__ccg"],
			'__rev': request_template["html_variables"]["__rev"],
			'__hsi': request_template["html_variables"]["__hsi"],
			'__comet_req': request_template["html_variables"]["__comet_req"],
			'fb_dtsg': request_template["html_variables"]["fb_dtsg"],
			'jazoest': request_template["html_variables"]["jazoest"],
			'lsd': request_template["html_variables"]["x-fb-lsd"],
			'__spin_r': request_template["html_variables"]["__rev"],
			'__spin_b': request_template["html_variables"]["__spin_b"],
			'__spin_t': request_template["html_variables"]["__spin_t"],
			'__jssesw': '1',
			'fb_api_caller_class' : "RelayModern",
			'variables': '{}',
			'server_timestamps': 'true'
		}


'''
def sliding_levenshtein(this_page_name, this_query_string):
	this_page_name_processed = this_page_name.lower()
	this_query_string_processed = this_query_string.lower()
	MIN_QUERY_STRING_LENGTH = 4
	if (len(this_query_string_processed) < MIN_QUERY_STRING_LENGTH):
		return False
	else:
		if (len(this_page_name_processed) < len(this_query_string_processed)):
			return (levenshtein(this_page_name_processed, this_query_string_processed) <= MAX_LEVENSHTEIN_DISTANCE)
		else:
			for i in range(len(this_page_name_processed)-len(this_query_string_processed)+1):
				if (levenshtein(this_page_name_processed[i:i+len(this_page_name_processed)], this_query_string) <= MAX_LEVENSHTEIN_DISTANCE):
					return True
			return False
'''

'''
	This function queries an exact keyword phrase, returning the raw response
'''
def meta_adlibrary_query_keyword_exact_phrase(variables, this_scrape_identity, up_to=int(time.time()), doc_id_override=None, stop_at_response=False, country_override=None):
	output = dict()
	# Set the request details
	applied_request_config = {
			"type" : "AdLibraryMobileFocusedStateProviderRefetchQuery",
			"variables" : mergedeep.merge(
				GLOBALS_CONFIG["meta_adlibrary"]["requests"]["ad_library_mobile_focused_state_provider_refetch_query"], 
				variables)
		}
	# Apply a date constraint to retrieve only the most relevant ads
	import pytz
	import datetime
	tz = pytz.timezone("Australia/Brisbane")
	applied_request_config["variables"]["startDate"] = {"min":None, "max": datetime.datetime.fromtimestamp(up_to, datetime.UTC).replace(tzinfo=tz).strftime('%Y-%m-%d')}
	# Conduct a doc_id override during testing
	if (doc_id_override is not None):
		applied_request_config["doc_id_override"] = doc_id_override
	# The response typically arrives in newline-separated parts.
	# We cannot be certain which index 'exactly' houses the data of interest
	# or whether new data will be attached at later stages - we will retrieve the
	# entire data dump, and filter what we can interpret
	response_content = meta_adlibrary_request_send(applied_request_config, this_scrape_identity,
		{ "ad_capture_appendage": GLOBALS_CONFIG["meta_adlibrary"]["ad_capture_appendage_alt"] }, country_override=country_override).content
	if (stop_at_response):
		ipdb.set_trace()
	output["response_raw"] = response_content.decode("utf-8")
	output["response_interpreted"] = { "success" : False, "json_raw" : list(), "json_interpreted" : list() }
	output["scrape_identity_uuid"] = this_scrape_identity["uuid"]

	'''
	# Deprecated: It's possible to retrieve results and simultaneously trigger the missing_variable_error_str
	if (missing_variable_error_str in output["response_raw"]):
		# We will not freeze the scraper in this case, but the scrape is considered an error nonetheless
		print("Found missing_variable_error_str in response_raw")
		output["response_interpreted"]["error"] = output["response_raw"]
	elif
	'''
	if (close_and_reopen_window_str in output["response_raw"]):
		if (VERBOSE): print("Found close_and_reopen_window_str in response_raw")
		output["response_interpreted"]["error"] = output["response_raw"]
	else:
		try:
			response_decoded = response_content.decode("utf-8").split("\n")
			#print(response_decoded)
			output["response_interpreted"]["json_raw"] = list()
			for x in response_decoded:
				try:
					this_sub_response = json.loads(x)
					# We can relax this original condition
					#if (("label" in this_sub_response) and ("AdLibraryMobileFocusedStateProvider" in this_sub_response["label"])):
					for this_edge in this_sub_response["data"]["ad_library_main"]["search_results_connection"]["edges"]:
						for this_result in this_edge["node"]["collated_results"]:
							output["response_interpreted"]["json_raw"].append(this_result)
					this_json_raw = output["response_interpreted"]["json_raw"]
					if (VERBOSE): print(f"\tNo. Results: {len(this_json_raw)}")
					output["response_interpreted"]["success"] = True
				except:
					if (__name__ == "__main__"):
						if (VERBOSE): print(traceback.format_exc())
					pass
			#output["response_interpreted"]["json_interpreted"] = output["response_interpreted"]["json_raw"]
			# If operating on query string...
			if ("queryString" in variables):
				output["response_interpreted"]["json_interpreted"] = list()
				# Filter the output to only those that match the query string
				query_string = variables["queryString"].lower()
				try: query_string = json.loads(variables["queryString"]).lower()
				except: pass 
				for this_result in output["response_interpreted"]["json_raw"]:
					if (sliding_levenshtein_pct(this_result["page_name"], query_string) > 0.75):
						output["response_interpreted"]["json_interpreted"].append(this_result)
					#if (levenshtein(this_result["page_name"].lower(), query_string) <= MAX_LEVENSHTEIN_DISTANCE):
					#	output["response_interpreted"]["json_interpreted"].append(this_result)
		except:
			if (VERBOSE): print(traceback.format_exc())
			output["response_interpreted"]["error"] = str(traceback.format_exc())

	if (VERBOSE): print("Scrape Success:", output["response_interpreted"]["success"])
	this_json_interpreted = output["response_interpreted"]["json_interpreted"]
	if (VERBOSE): print(f"\tNo. Results (filtered): {len(this_json_interpreted)}")
	# If the response is interpreted, we can isolate the links from within the JSON -
	# again, we cannot anticipate the links included in the schema, and instead resort to
	# Regex
	output["response_interpreted"]["outlinks"] = None
	try:
		# Isolate all links (this is used to find medias in proceeding steps)
		output["response_interpreted"]["outlinks"] = list(set(re.findall(
			r'https\:\/\/.*?(?=\"| |\\n|\n|\\t|\t|$)', json.dumps(output["response_interpreted"]["json_interpreted"]))))
		n_outlinks = len(output["response_interpreted"]["outlinks"])
		if (VERBOSE): print(f"\tNo. Outlinks: {n_outlinks}")
	except:
		pass
	return output

# Simulate requests on the tentative doc IDs to determine the correct doc ID
def meta_adlibrary_request_template_verify_apply_doc_id(this_scrape_identity, tentative_doc_ids):
	success = False
	for this_tentative_doc_id in tentative_doc_ids:
		if (len(this_tentative_doc_id) > 0):
			output = meta_adlibrary_query_keyword_exact_phrase(
						{"queryString" : json.loads("\""+TEST_KEYWORD+"\"")}, this_scrape_identity, doc_id_override=this_tentative_doc_id)
			if (VERBOSE): print("Applying trace to avoid capture...")
			if (output["response_interpreted"]["success"]):
				# Cache
				success = True
				this_scrape_identity["request_template"]["html_variables"]["doc_id"] = this_tentative_doc_id
				break
	return success, this_scrape_identity

'''
	This function verifies that the Meta Facebook request template (that is to be cached)
	is sound
'''
def meta_adlibrary_request_template_verify(this_scrape_identity):
	success = False
	# This section has been commented out to avoid 
	# 
	#try: assert (len(this_scrape_identity["request_template"]["cookies"]["c_user"]) > 5)
	#except:
	#	print("Failed to assert user cookie")
	#	ipdb.set_trace()
	tentative_doc_ids = this_scrape_identity["request_template"]["html_variables"]["tentative_doc_ids"]
	try: assert (len(tentative_doc_ids) > 0)
	except:
		if (VERBOSE): print("Failed to assert doc ID")
		ipdb.set_trace()

	# Simulate requests on the tentative doc IDs to determine the correct doc ID
	success, this_scrape_identity = meta_adlibrary_request_template_verify_apply_doc_id(this_scrape_identity, tentative_doc_ids)

	# TODO - attempt to adjust the scrape identity by switching out the c_user cookie for a dpr cookie
	if (not success):
		if (VERBOSE): print("Initial attempt failed - retrying with cookie hotfix:")
		del this_scrape_identity["request_template"]["cookies"]["c_user"]
		this_scrape_identity["request_template"]["cookies"]["dpr"] = "1"
		success, this_scrape_identity = meta_adlibrary_request_template_verify_apply_doc_id(this_scrape_identity, tentative_doc_ids)

	if (TEST_IS_DIAGNOSTIC):
		ipdb.set_trace()

	if (VERBOSE): print("Success:", success)
	return success, this_scrape_identity

'''
	This function caches the request template
'''
def meta_adlibrary_request_template_cache():
	this_uuid = sys.argv[2]
	driver = None
	scrape_identity_name = f"scrape_identities/{this_uuid}.json"
	try:
		this_scrape_identity = json_s3_load_holding(scrape_identity_name)
		this_public_ip_address = this_scrape_identity["public_ip_address_meta_adlibrary"]
		proxies = this_scrape_identity["proxies"]
		driver = selenium_driver(this_public_ip_address, LOCAL_TINYPROXY_PATH)
		if (TEST_IS_DIAGNOSTIC):
			ipdb.set_trace() # TODO - this is set as a workaround for manually allowing MV2 extensions while we develop a bigger workaround for future
		# Generate the template for a request
		cookies = meta_adlibrary_extract_base_cookies(driver)
		html_variables = meta_adlibrary_extract_misc_html_variables(driver, proxies)
		request_template = {
				"cookies" : cookies,
				"headers" : GLOBALS_CONFIG["meta_adlibrary"]["base_headers"], # base headers "x-asbd-id" : '129477'
				"html_variables" : html_variables
			}
		request_template["data"] = meta_adlibrary_extract_base_data(request_template)
		this_scrape_identity["request_template"] = request_template
		verification_value, this_scrape_identity = (meta_adlibrary_request_template_verify(this_scrape_identity))
		this_scrape_identity["valid"] = verification_value
	except:
		if (VERBOSE): print(traceback.format_exc())
		this_scrape_identity["valid"] = False
		#send_gmail("obei@qut.edu.au", "Meta Ad Library Caching Failure", "...")
	this_scrape_identity["last_cached_at"] = str(int(time.time()))
	json_s3_save_holding(scrape_identity_name, this_scrape_identity)
	try: 
		driver.quit()
		import psutil
		for proc in psutil.process_iter():
			try: 
				if (proc.name() == "chromedriver"): proc.kill()
			except: pass
	except: pass

'''
	In the event of failure, send an email...
'''
def meta_adlibrary_scrape_failure(this_scrape_identity, quote=str()):
	# Ensure no future scraping is undertaken if the scraper fails
	send_gmail("obei@qut.edu.au", f"Meta Ad Library Scraper Failure - {quote}", "...")

'''
	This function runs a name-check over the Meta Ad Library
'''
def routine_meta_adlibrary_namecheck(_this_scrape_identity, queryString, country_override=None):
	this_scrape_identity = dict(_this_scrape_identity)
	try:
		if (country_override is None): country_override = GLOBALS_CONFIG["meta_adlibrary"]["request_config_base"]["ad_capture_appendage"]["country"]
		# Apply the query
		applied_request_config = {
				"type" : "useAdLibraryTypeaheadSuggestionDataSourceQuery", 
				"variables" :{"queryString":queryString,"isMobile":False,"country":country_override,"adType":"ALL"}
			}
		request_config_base = mergedeep.merge(GLOBALS_CONFIG["meta_adlibrary"]["request_config_base"], dict())
		this_request_preparation = meta_adlibrary_prepare_request(
			mergedeep.merge(request_config_base, applied_request_config), this_scrape_identity, country_override=country_override)

		this_request_preparation["cookies"] = (this_request_preparation["cookies"] 
												| {
													#"wl_cbv" : 'v2%3Bclient_version%3A2857%3Btimestamp%3A1751426832',
													'dpr': '1',
													'ar_debug': '1',
												})

		this_request_preparation["headers"] = (this_request_preparation["headers"] 
												| {
													'cache-control': 'no-cache',
	    											'pragma': 'no-cache'
												})
		this_request_preparation["data"]["doc_id"] = this_scrape_identity["request_template"]["html_variables"]["tentative_doc_ids_typeahead"][0]

		# Send the request
		response = requests.post("https://www.facebook.com/api/graphql/", 
				proxies=this_scrape_identity["proxies"]["meta_adlibrary"], # We have to deliberately set this for Meta Ad Library
				cookies=this_request_preparation["cookies"],
				headers=this_request_preparation["headers"],
				data=this_request_preparation["data"]
			)
		return json.loads(response.content.decode("utf-8"))
	except:
		if (VERBOSE): print(traceback.format_exc())
		return {"error" : str(traceback.format_exc())}

'''
	This function runs a scrape of the Meta Ad Library
'''
def routine_meta_adlibrary_scrape(_this_scrape_identity, query={ "queryString" : json.dumps("Jojubi Saddlery") }, up_to=int(time.time()), stop_at_response=False, country_override=None):
	this_scrape_identity = dict(_this_scrape_identity)
	# The request is undertaken on a given keyword
	output = meta_adlibrary_query_keyword_exact_phrase(query, this_scrape_identity, up_to=up_to, stop_at_response=stop_at_response, country_override=country_override)
	this_scrape_identity["scrape_log"].append({"timestamp" : int(time.time())})
	if (("Rate limit exceeded" in output["response_raw"])
		or ("Log in to continue" in output["response_raw"])
		or ("Please review your account" in output["response_raw"])):
		if (VERBOSE): print(output)
		this_scrape_identity["valid"] = False
		meta_adlibrary_scrape_failure(this_scrape_identity, quote=f"{this_scrape_identity['uuid']} - Rate Limit Exceeded")
		output = None
	elif ("response_interpreted" in output):
		if (("error" in output["response_interpreted"]) 
					and (close_and_reopen_window_str in output["response_interpreted"]["error"])):
			this_scrape_identity["valid"] = False
			meta_adlibrary_scrape_failure(this_scrape_identity, quote=f"{this_scrape_identity['uuid']} - Cache Refresh Required")
			output = None
		elif (not output["response_interpreted"]["success"]):
			# We do not freeze the scrape identity based on a missing_variable_error_str
			if (missing_variable_error_str in output["response_raw"]):
				pass
			else:
				this_scrape_identity["valid"] = False
				meta_adlibrary_scrape_failure(this_scrape_identity, quote=f"{this_scrape_identity['uuid']} - Failed Interpretation")

	# Save the result to the scrape identity
	this_scrape_identity["scrape_log"] = scrape_log_clean(this_scrape_identity["scrape_log"])
	json_s3_save_holding(f"scrape_identities/{this_scrape_identity['uuid']}.json", this_scrape_identity)
	return output

def scrape_identity_shallow_remove():
	this_uuid = sys.argv[2]
	print(f"scrape_identities/{this_uuid}.json")
	json_s3_delete_holding(f"scrape_identities/{this_uuid}.json")

'''

	TODO
		
		list available scrape identities

		append to scrape log

		when max is reached - drop off scrape log




	ready to be recached but not yet provisioned
	
	python scrape_meta_adlibrary.py provision xxx 3.106.201.18


	
	
	
	
	
	
	
	


	recached

	python scrape_meta_adlibrary.py cache 42c1fda6-4bc3-4f35-9474-88a7aba2a570 3.27.234.87
	python scrape_meta_adlibrary.py cache 540c318a-f390-4ad4-bd52-730cf651444d 13.55.177.64
	python scrape_meta_adlibrary.py cache ae8b0577-b994-47be-9066-6c1ee01b1e2e 13.210.16.56
	python scrape_meta_adlibrary.py cache 9d0bcfcc-815e-4ddd-a83c-af4cf0f2e253 13.239.25.161
	python scrape_meta_adlibrary.py cache 39031ae3-0e22-46d8-8937-e77e6ae8da2a 13.55.107.160
	python scrape_meta_adlibrary.py cache 6b5d3180-e54c-4fd5-aeec-c871da3fd0eb 16.176.215.101
	python scrape_meta_adlibrary.py cache da7ff82a-a5b1-41b4-9aea-2e1fe3af7205 54.79.183.175
	python scrape_meta_adlibrary.py cache 2ae3d4dc-362c-4ea9-91a1-0adc524da5cd 52.65.93.82
	python scrape_meta_adlibrary.py cache b67377a4-8d1d-4b03-bb94-b33708597318 3.25.55.11
	python scrape_meta_adlibrary.py cache 2826f6a4-1c00-4cc2-b587-de6f9ed9dca9 3.106.117.44
	python scrape_meta_adlibrary.py cache dc960f44-1363-434e-9b17-587d53ae035c 16.176.230.164
	python scrape_meta_adlibrary.py cache 28c8b0c7-93cf-4e9b-b1c1-a97982b5f9ca 3.27.236.52
	python scrape_meta_adlibrary.py cache 00da51f8-fd19-4fe4-aed8-2abb34420058 15.134.138.179
	python scrape_meta_adlibrary.py cache 748ed721-10fb-4cb3-a13d-4872e49a639c 16.176.169.11
	python scrape_meta_adlibrary.py cache 1aee0ca5-50b8-449a-b082-0a85ed5a71b6 54.206.179.87
	python scrape_meta_adlibrary.py cache 37c7864b-c217-4cf8-88e7-1a6591f81180 3.107.160.181
	python scrape_meta_adlibrary.py cache 2eaa89ef-659f-479f-aa61-47b2c16f740d 3.27.236.52
	python scrape_meta_adlibrary.py cache 354f4c76-4e72-4be4-8627-26f6fba08a90 3.26.168.75
	python scrape_meta_adlibrary.py cache 8c0a267c-a31f-40fb-971d-0bc1c74b7c26 3.25.172.83 - Abdul Obeid (old) abdulobeid@hotmail.com
	python scrape_meta_adlibrary.py cache 150badab-86fb-4908-98ba-db6437d02159 13.211.68.246 - Aisha Ay trushyt@live.com abdulian_123@hotmail.com Qazer



python scrape_meta_adlibrary.py cache 00da51f8-fd19-4fe4-aed8-2abb34420058 gem
python scrape_meta_adlibrary.py cache 150badab-86fb-4908-98ba-db6437d02159 home
python scrape_meta_adlibrary.py cache 1aee0ca5-50b8-449a-b082-0a85ed5a71b6 opal
python scrape_meta_adlibrary.py cache 2826f6a4-1c00-4cc2-b587-de6f9ed9dca9 travel
python scrape_meta_adlibrary.py cache 28c8b0c7-93cf-4e9b-b1c1-a97982b5f9ca show
python scrape_meta_adlibrary.py cache 2ae3d4dc-362c-4ea9-91a1-0adc524da5cd dinosaur
python scrape_meta_adlibrary.py cache 2eaa89ef-659f-479f-aa61-47b2c16f740d car
python scrape_meta_adlibrary.py cache 354f4c76-4e72-4be4-8627-26f6fba08a90 time
python scrape_meta_adlibrary.py cache 37c7864b-c217-4cf8-88e7-1a6591f81180 dinosaur
python scrape_meta_adlibrary.py cache 39031ae3-0e22-46d8-8937-e77e6ae8da2a car
python scrape_meta_adlibrary.py cache 42c1fda6-4bc3-4f35-9474-88a7aba2a570 card
python scrape_meta_adlibrary.py cache 540c318a-f390-4ad4-bd52-730cf651444d gem
python scrape_meta_adlibrary.py cache 6b5d3180-e54c-4fd5-aeec-c871da3fd0eb time
python scrape_meta_adlibrary.py cache 748ed721-10fb-4cb3-a13d-4872e49a639c show
python scrape_meta_adlibrary.py cache 8c0a267c-a31f-40fb-971d-0bc1c74b7c26 home
python scrape_meta_adlibrary.py cache 9d0bcfcc-815e-4ddd-a83c-af4cf0f2e253 opal
python scrape_meta_adlibrary.py cache ae8b0577-b994-47be-9066-6c1ee01b1e2e travel
python scrape_meta_adlibrary.py cache b67377a4-8d1d-4b03-bb94-b33708597318 gem
python scrape_meta_adlibrary.py cache da7ff82a-a5b1-41b4-9aea-2e1fe3af7205 home
python scrape_meta_adlibrary.py cache dc960f44-1363-434e-9b17-587d53ae035c dinosaur


'''
def meta_adlibrary_test_scrape():
	if (VERBOSE): print("Retrieving scrape identity...")
	this_scrape_identity = get_available_scrape_identity(platform="FACEBOOK")
	if (VERBOSE): print(json.dumps(this_scrape_identity,indent=3))
	if (this_scrape_identity is not None):
		this_scrape_output = routine_meta_adlibrary_scrape(this_scrape_identity, 
			query={ "queryString" : json.dumps("car") }, up_to=int(time.time()))
		if (VERBOSE): print(json.dumps(this_scrape_output,indent=3))
	ipdb.set_trace()

def meta_adlibrary_test_scrape_forced():
	if (VERBOSE): print("Retrieving scrape identity...")
	this_scrape_identity = get_available_scrape_identity(platform="FACEBOOK", specify=sys.argv[2])
	if (VERBOSE): print(json.dumps(this_scrape_identity,indent=3))
	if (this_scrape_identity is not None):
		this_scrape_output = routine_meta_adlibrary_scrape(this_scrape_identity, 
			query={ "queryString" : json.dumps("car") }, up_to=int(time.time()))
		if (VERBOSE): print(json.dumps(this_scrape_output,indent=3))
	ipdb.set_trace()

'''
	mass downloader

		check scrape identities for vendor

		for each link - detrmine type

		then download and place in position
'''

if (__name__ == "__main__"):
	'''
	this_scrape_identity = get_available_scrape_identity(platform="FACEBOOK")
	this_scrape_output = routine_meta_adlibrary_scrape(this_scrape_identity, 
		query={ "queryString" : json.dumps("rewe") }, up_to=int(time.time()), stop_at_response=False, country_override="DE")
	print(json.dumps(this_scrape_output,indent=3))
	ipdb.set_trace()
	#meta_adlibrary_test_scrape_non_au()
	'''

	
	AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

	if (len(sys.argv) > 1):
		{
			"provision" : provision_for_scrape_identity,
			"shallow_remove" : scrape_identity_shallow_remove, # does not delete aws resources
			"cache" : meta_adlibrary_request_template_cache,
			"test" : meta_adlibrary_test_scrape,
			"test_forced" : meta_adlibrary_test_scrape_forced
		}[sys.argv[1]]()
	'''
	this_scrape_identity = get_available_scrape_identity(platform="FACEBOOK", divert_designation_block=True)
	response = routine_meta_adlibrary_namecheck(this_scrape_identity, "worldf")
	ipdb.set_trace()
	'''






