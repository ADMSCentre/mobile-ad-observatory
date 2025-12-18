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
import random
import urllib
import shutil
import re
import uuid
import mergedeep
import smtplib
import ssl
from email.message import EmailMessage
import traceback

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

VERBOSE = False

def send_gmail(arg_to, arg_subject, arg_body):
    try:
        # Hardcoded credentials...
        gmail_user = 'dmrc.do.not.reply@gmail.com'
        gmail_app_password = open("gmail_app_password").read()
        # Message definition...
        message = MIMEMultipart()
        message['From'] = gmail_user
        message['To'] = arg_to
        message['Subject'] = arg_subject
        message.attach(MIMEText(arg_body, 'html'))
        # Server definition...
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(gmail_user, gmail_app_password)
        server.send_message(message)
        server.quit()
    except: pass

if (__name__ == "__main__"):
	import ipdb

GLOBALS_CONFIG = json.loads(open(os.path.join(os.getcwd(), "configs", "globals.json")).read())

##############################################################################################################################
##############################################################################################################################
### AWS
##############################################################################################################################
##############################################################################################################################

# Load up the necessary AWS infrastructure
# Note: On remote infrastructures, we don't authenticate as the Lambda handler will have the necessary
# permissions built into it
AWS_REQUIRED_RESOURCES = ["s3"]
AWS_RESOURCE = None
AWS_CLIENT = None
def aws_load(running_locally=False):
	credentials_applied = dict()
	if (running_locally):
		# Running locally
		credentials = boto3.Session(profile_name=GLOBALS_CONFIG["aws"]["AWS_PROFILE"]).get_credentials()
		credentials_applied = {
				"region_name" : GLOBALS_CONFIG["aws"]["AWS_REGION"],
				"aws_access_key_id" : credentials.access_key,
				"aws_secret_access_key" : credentials.secret_key
			}
	AWS_RESOURCE = {k : boto3.resource(k, **credentials_applied) for k in AWS_REQUIRED_RESOURCES}
	AWS_CLIENT = {k : boto3.client(k, **credentials_applied) for k in AWS_REQUIRED_RESOURCES}
	return AWS_CLIENT, AWS_RESOURCE

AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

HOLDING_BUCKET = GLOBALS_CONFIG["holding_bucket"]

def get_list_objects_v2(Bucket=None, Prefix=None):
	result = list()
	paginator = AWS_CLIENT["s3"].get_paginator('list_objects_v2')
	pages = paginator.paginate(Bucket=Bucket, Prefix=Prefix)
	for page in pages:
		if ("Contents" in page):
			result.extend(page['Contents'])
	return {"Contents" : result}

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

##############################################################################################################################
##############################################################################################################################
### SELENIUM FUNCTIONALITY
##############################################################################################################################
##############################################################################################################################

LOCAL_TINYPROXY_PATH = os.path.join(GLOBALS_CONFIG["tinyproxy_path"])

def get_all_scrape_identities():
	scrape_identities = get_list_objects_v2(Bucket="fta-mobile-observations-holding-bucket", Prefix="scrape_identities/")
	scrape_identities_instantiated = list()
	for x in scrape_identities["Contents"]:
		scrape_identities_instantiated.append(json.loads(AWS_RESOURCE["s3"].Object("fta-mobile-observations-holding-bucket",x["Key"]).get()['Body'].read()))
	return scrape_identities_instantiated
'''
	Construct a selenium driver
'''
def selenium_driver(this_public_ip_address, local_tinyproxy_path=LOCAL_TINYPROXY_PATH):
	# Note that these packages are only loaded on direct call, as remote functionality won't support it
	#from selenium import webdriver
	from seleniumwire import webdriver
	from selenium.webdriver.chrome.options import Options
	scrape_identities = get_all_scrape_identities()
	this_scrape_identity = [x for x in scrape_identities if (x["public_ip_address_meta_adlibrary"] == this_public_ip_address)][0]
	EC2_INSTANCE_SELENIUM_DRIVER_DIRECTORY = os.path.join(local_tinyproxy_path, "selenium_driver_"
															+ this_public_ip_address.replace(".", "_") + "_new")

	seleniumwire_options = {
	    'proxy': this_scrape_identity["proxies"]["meta_adlibrary"] |  {'no_proxy': 'localhost,127.0.0.1'}
	}
	options = Options()
	options.add_argument("--disable-blink-features=AutomationControlled")
	options.add_argument(f"user-data-dir={EC2_INSTANCE_SELENIUM_DRIVER_DIRECTORY}")
	driver = webdriver.Chrome( options=options, seleniumwire_options=seleniumwire_options)
	return driver


'''
	Puppeteer a Selenium instance
'''
def selenium_puppeteer(local_tinyproxy_path=LOCAL_TINYPROXY_PATH):
	driver = selenium_driver(sys.argv[2], local_tinyproxy_path)
	driver.get("https://whatismyip.com/")
	ipdb.set_trace()
	driver.quit()

'''
	This function assembles the proxy strings necessary to run 'requests' module calls with the
	denoted proxy
'''
def assemble_proxy_string(this_proxy):
	protocols = ["http", "https"]
	proxies = {protocol : 
		(f'http://{this_proxy["proxy_username"]}:{this_proxy["proxy_password"]}'
			+ f'@{this_proxy["public_ip_address"]}:{this_proxy["proxy_port"]}/') for protocol in protocols}
	return proxies

'''
	Retrieve the Proxy for an IP address (while assessing it in the process)
'''
def retrieve_proxy_for_ip_address(this_public_ip_address, local_tinyproxy_path=LOCAL_TINYPROXY_PATH):
	try:
		this_candidate = [x for x in json.loads(open(os.path.join(
			local_tinyproxy_path, "local_data", "ec2_instances.json")).read())["active_ec2_instances"]
								if (x["public_ip_address"] == this_public_ip_address)][0]

		if (this_candidate["tinyproxy"] != "ACTIVE"):
			if (VERBOSE): print("Proxy is not active!")
			return None
		else:
			return this_candidate
	except:
		if (VERBOSE): print(traceback.format_exc())
		return None


##############################################################################################################################
##############################################################################################################################
### GENERAL SCRAPE AND PROXY FUNCTIONALITY
##############################################################################################################################
##############################################################################################################################

'''
	This function accepts a list of scrape identities, and selects the scrape identity
	with the smallest time interval between now and its resumption
'''
def scrape_log_clean(this_scrape_log):
	AN_HOUR = 60 * 60
	return [y for y in this_scrape_log if (abs(y["timestamp"] - int(time.time())) <= AN_HOUR)]

'''
	This function designates an available scrape identity for the given platform,
	assessing time and maximum scrape constraints
'''
def designate_available_scrape_identity(available_scrape_identities, scraper_config, assert_lock_check=False):
	max_scrapes_in_hour = scraper_config["SCRAPE_MAX_SCRAPES_IN_HOUR"]
	threshold_interval = scraper_config["SCRAPE_THRESHOLD_INTERVAL"]
	timeout_for_throttling = scraper_config["SCRAPE_TIMEOUT_FOR_THROTTLING"]
	random_overthrottle = int(timeout_for_throttling * (random.randint(1,100)/100.0)) # We apply this to add some randomness to behaviour
	# Note: Ideally we would like to evaluate the scrape log only once, however we have to do it on the fly, as the values are contextualised
	# to the time of execution
	exceeded_calls_in_hour = [(len(scrape_log_clean(x["scrape_log"])) > max_scrapes_in_hour) for x in available_scrape_identities]
	last_calls = [((int(time.time()) - (threshold_interval*2)) if (len(x["scrape_log"]) == 0) else
						sorted(x["scrape_log"], key=lambda d: d["timestamp"])[-1]["timestamp"]) for x in available_scrape_identities]
	exceeded_calls_in_minute = [(abs(x - int(time.time())) < threshold_interval) for x in last_calls]
	available_scrape_identities_to_designate = [available_scrape_identities[i]
		for i in range(len(available_scrape_identities)) if ((not exceeded_calls_in_hour[i]) and (not exceeded_calls_in_minute[i]))]
	if (assert_lock_check):
		# Reload the scrape identities and check if any are locked (as the designation may've been delayed)
		available_scrape_identities_to_designate = [json_s3_load_holding(f"scrape_identities/{x['uuid']}.json") for x in available_scrape_identities_to_designate]
		available_scrape_identities_to_designate = [x for x in available_scrape_identities_to_designate if (not x["locked"])]

				
	if (len(available_scrape_identities_to_designate) > 0):
		random.shuffle(available_scrape_identities_to_designate)
		#ipdb.set_trace()
		return { "scrape_identity" : available_scrape_identities_to_designate[0] }
	else:
		# If no scrape identity is available here, it is either due to maxxing out by the hour, or maxxing out by the minute
		if (all(exceeded_calls_in_hour)):
			# If all scrapers have exceeded their hourly limits, then apply a generic timeout and reevaluate afterwards
			return { "timeout" : (timeout_for_throttling + random_overthrottle) }
		else:
			desired_timestamp_for_resumption = (int(time.time()) - (threshold_interval + 1))
			the_very_last_call_on_an_available_scraper = max(last_calls)
			# The difference between the desired timestamp for resumption and the timestamp of the very last call is the number
			# of seconds to wait before resuming the scrape process
			seconds_to_wait = abs(desired_timestamp_for_resumption - the_very_last_call_on_an_available_scraper)
			return { "timeout" : (seconds_to_wait + random_overthrottle) }
'''
def dummy_process():
	specify = None
	assert_lock_check = True
	platform = "FACEBOOK"
	vendor = GLOBALS_CONFIG["platform_vendor_mappings"][platform]

	scrape_identities = get_all_scrape_identities()
	available_scrape_identities = [x for x in scrape_identities 
		if (((specify is not None) or ((x["vendor"] == vendor.upper()) and (x["valid"])))
			and ((not assert_lock_check) or ((assert_lock_check) and ((not "locked" in x) or (not x["locked"])))))]
	scraper_config = GLOBALS_CONFIG[vendor]["scraper_config"]
	
	if (True):
		available_scrape_identities = [x for x in available_scrape_identities 
					if (abs(int(time.time()) - int(x["last_cached_at"])) <= scraper_config["CACHE_EXPIRY_SECONDS"])]
	
	actually_available_identities = [x["uuid"] for x in available_scrape_identities]
	ipdb.set_trace()
	designate_available_scrape_identity(available_scrape_identities, scraper_config)

	max_scrapes_in_hour = scraper_config["SCRAPE_MAX_SCRAPES_IN_HOUR"]
	threshold_interval = scraper_config["SCRAPE_THRESHOLD_INTERVAL"]
	timeout_for_throttling = scraper_config["SCRAPE_TIMEOUT_FOR_THROTTLING"]
	random_overthrottle = int(timeout_for_throttling * (random.randint(1,100)/100.0)) # We apply this to add some randomness to behaviour
	this_identity = json.loads(open("dummy.json").read())
	exceeded_calls_in_hour = (len(scrape_log_clean(this_identity["scrape_log"])) > max_scrapes_in_hour)

	last_call = ((int(time.time()) - (threshold_interval*2)) if (len(this_identity["scrape_log"]) == 0) else
						sorted(this_identity["scrape_log"], key=lambda d: d["timestamp"])[-1]["timestamp"])

	exceeded_calls_in_minute = (abs(last_call - int(time.time())) < threshold_interval)
	ipdb.set_trace()

dummy_process()
ipdb.set_trace()
'''
'''
	This function retrieves an available scrape identity for a given platform
'''
def get_available_scrape_identity(platform="FACEBOOK", specify=None, ignore_last_cache_at=False, divert_designation_block=False, break_on_n_attempts=False, assert_lock_check=False):
	vendor = GLOBALS_CONFIG["platform_vendor_mappings"][platform]
	scraper_config = GLOBALS_CONFIG[vendor]["scraper_config"]
	# Load in the scrape identities
	scrape_identities = get_all_scrape_identities()
	available_scrape_identities = [x for x in scrape_identities 
		if (((specify is not None) or ((x["vendor"] == vendor.upper()) and (x["valid"])))
			and ((not assert_lock_check) or ((assert_lock_check) and ((not "locked" in x) or (not x["locked"])))))]
	if (specify is not None):
		available_scrape_identities = [x for x in available_scrape_identities if (x["uuid"] == specify)]
		if (VERBOSE): print(json.dumps(available_scrape_identities, indent=3))

	if (len(available_scrape_identities) == 0):
		if (VERBOSE): print("No available scrape identity...")
		if (__name__ == "__main__"):
			ipdb.set_trace()
		return None
	if (not ignore_last_cache_at):
		filtered_available_scrape_identities = [x for x in available_scrape_identities 
					if (abs(int(time.time()) - int(x["last_cached_at"])) <= scraper_config["CACHE_EXPIRY_SECONDS"])]
		# Housekeeping - ensure expired scrapers are flagged as invalid
		if (len(available_scrape_identities) != len(filtered_available_scrape_identities)):
			to_flag_as_invalid = [x for x in available_scrape_identities if (not x["uuid"] in [y["uuid"] for y in filtered_available_scrape_identities])]
			for x in to_flag_as_invalid:
				scrape_identity_key = f"scrape_identities/{x['uuid']}.json"
				if (VERBOSE): print(f"Updating scrape identity for {x['uuid']}")
				json_s3_save_holding(scrape_identity_key, json_s3_load_holding(scrape_identity_key) | {"valid" : False, "locked" : False})
		available_scrape_identities = filtered_available_scrape_identities
	else:
		if (VERBOSE): print("Ignoring 'last cache' condition")
	if (len(available_scrape_identities) == 0):
		if (VERBOSE): print("Please refresh caches and then continue...")
		if (__name__ == "__main__"):
			ipdb.set_trace()
		return None
	if (len(available_scrape_identities) > 0):
		if (divert_designation_block):
			designated_scrape_identity_container = designate_available_scrape_identity(available_scrape_identities, scraper_config, assert_lock_check=assert_lock_check)
			if ("timeout" in designated_scrape_identity_container):
				return "DIVERTED"
			else:
				return designated_scrape_identity_container["scrape_identity"]
		else:
			# Get the scraper who is within the scrape identity time threshold
			designated_scrape_identity_container = dict()
			n_attempts = int()
			max_n_attempts = 3
			while (not "scrape_identity" in designated_scrape_identity_container):
				if ((n_attempts > max_n_attempts) and (break_on_n_attempts)):
					return None
				designated_scrape_identity_container = designate_available_scrape_identity(available_scrape_identities, scraper_config, assert_lock_check=assert_lock_check)
				if ("timeout" in designated_scrape_identity_container):
					# Pause the scrape process while we wait for a scrape identitiy
					if (VERBOSE): print(f"Pausing execution to allow for timeout ({designated_scrape_identity_container['timeout']})...")
					time.sleep(designated_scrape_identity_container['timeout'])
					n_attempts += 1
			return designated_scrape_identity_container["scrape_identity"]
	else:
		# 
		if (__name__ == "__main__"):
			ipdb.set_trace()
		return None


if (__name__ == "__main__"):
	AWS_CLIENT, AWS_RESOURCE = aws_load((__name__ == "__main__"))

	if (len(sys.argv) > 1):
		{
			"puppeteer" : selenium_puppeteer,
		}[sys.argv[1]]()
