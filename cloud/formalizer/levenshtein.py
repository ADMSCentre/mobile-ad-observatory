import math

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
	this script returns 100% match if one string exists within the other
'''
def sliding_levenshtein_pct(s1, s2):
	larger_string = (s1 if (len(s2) < len(s1)) else s2)
	smaller_string = (s2 if (len(s2) < len(s1)) else s1)
	larger_string = larger_string.lower()
	smaller_string = smaller_string.lower()
	MIN_QUERY_STRING_LENGTH = 4
	if (len(smaller_string) < MIN_QUERY_STRING_LENGTH):
		return False
	else:
		slides = [larger_string[i:i+len(smaller_string)] for i in range(len(larger_string)-len(smaller_string)+1)]
		levenshtein_results = [(x, smaller_string) for x in slides]
		levenshtein_results = [levenshtein(x, smaller_string) for x in slides]
		return ((1 - (min(levenshtein_results) / len(smaller_string))) * (len(smaller_string) / len(larger_string)))

if (__name__ == "__main__"):
	import ipdb
	print(sliding_levenshtein_pct("For You", "Steelcase@ EOFor You Sale"))
	ipdb.set_trace()