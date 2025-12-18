'''

	at some indefinite stage, the content for this step is created and loaded back into the rdo object - TODO




	the process goes as follows - an incomplete RDO is loaded up

	it is then examnined to determine what platform it comes from -this affects the chatgpt prompt

	if a commercial content library scraper process exists for that platform, it is then scraped

		in the case of facebook, the advertiser name needs to be deduced and turned into a prompt, the following things are considered:

			ocr texts and their confidence

			advertisement type

			ad frames

		this forms the prompt that is given to chatgpt - the response yields a number of advertiser names that are placed into a tentative scrape

	


	a separate process wakes up, checks for tentative scrapes and then allocates them to scraper sub-instances



		in the facebook case, a scraper sub-instance calls a proxy attached to a dummy facebook account

		it scrapes for the advertisement data and runs an associated download as well

		on successful completion, it runs a callback to signal that the tentative scrape is now complete

		the rdo is then constructed


	result is retrieved and attached





	
	REEL_FOOTER_BASED
	REEL_BASED
	MARKETPLACE_BASED
	STORY_BASED
	FEED_BASED



'''