if (__name__ == "__main__"):
	import ipdb
from levenshtein import *

def rect_area(r):
	x1, y1, x2, y2 = r
	return max(0, x2 - x1) * max(0, y2 - y1)

def intersect(r1, r2):
	if ((r1 is None) or (r2 is None)):
		return None
	x1 = max(r1[0], r2[0])
	y1 = max(r1[1], r2[1])
	x2 = min(r1[2], r2[2])
	y2 = min(r1[3], r2[3])
	if x1 < x2 and y1 < y2:
		return (x1, y1, x2, y2)
	return None

def compute_union_area(rects):
	"""Union area via vertical sweep line."""
	events = []  # (x, type, y1, y2)
	for x1, y1, x2, y2 in rects:
		events.append((x1, 1, y1, y2))  # enter
		events.append((x2, -1, y1, y2))  # exit

	events.sort()
	active_intervals = []
	prev_x = 0
	total_area = 0

	def vertical_covered_length(intervals):
		if not intervals:
			return 0
		intervals.sort()
		merged = [intervals[0]]
		for start, end in intervals[1:]:
			last = merged[-1]
			if start <= last[1]:
				merged[-1] = (last[0], max(last[1], end))
			else:
				merged.append((start, end))
		return sum(e - s for s, e in merged)

	for x, typ, y1, y2 in events:
		dx = x - prev_x
		if dx > 0:
			total_area += dx * vertical_covered_length(active_intervals)
		if typ == 1:
			active_intervals.append((y1, y2))
		else:
			active_intervals.remove((y1, y2))
		prev_x = x

	return total_area

def compute_intersection_area(setA, setB):
	"""Efficient pairwise intersection using bounding box pruning."""
	total = 0
	setB_sorted = sorted(setB, key=lambda r: r[0])  # sort by x1
	for a in setA:
		a_x1, a_y1, a_x2, a_y2 = a
		for b in setB_sorted:
			if b[0] >= a_x2:
				break  # B starts after A ends
			if b[2] <= a_x1:
				continue  # B ends before A starts
			if b[3] <= a_y1 or b[1] >= a_y2:
				continue  # no vertical overlap
			i = intersect(a, b)
			if i:
				total += rect_area(i)
	return total

def exact_overlap_percentage_optimized(setA, setB):
	areaA = compute_union_area(setA)
	areaB = compute_union_area(setB)
	inter = compute_intersection_area(setA, setB)
	union = areaA + areaB - inter
	if union == 0:
		return 0.0
	return (inter / union)

def ocr_to_rect(ocr_obj):
	return (ocr_obj["x"], ocr_obj["y"], (ocr_obj["x"] + ocr_obj["w"]), (ocr_obj["y"] + ocr_obj["h"]))

def get_screen_dimensions(metadata_obj, this_frame_n):
	screen_dimensions = metadata_obj["nameValuePairs"]["systemInformation"]["internalJSONObject"]["nameValuePairs"]["screenDimensions"]["internalJSONObject"]["nameValuePairs"]
	screen_dimensions = {k:int(v) for k,v in screen_dimensions.items()}
	return screen_dimensions

def get_composite_bbox(metadata_obj, this_frame_n):
	return  metadata_obj["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][this_frame_n]["internalJSONObject"]["nameValuePairs"]["inference"]["internalJSONObject"]["nameValuePairs"]["boundingBoxCropped"]["internalJSONObject"]["nameValuePairs"]

def get_composite_dimensions(metadata_obj, this_frame_n):
	screen_dimensions = get_screen_dimensions(metadata_obj, this_frame_n)
	composite_bbox = get_composite_bbox(metadata_obj, this_frame_n)
	return ((composite_bbox["w"]*screen_dimensions["w"]), (composite_bbox["h"]*screen_dimensions["h"]))


def get_composite_location(metadata_obj, this_frame_n):
	screen_dimensions = get_screen_dimensions(metadata_obj, this_frame_n)
	composite_bbox = get_composite_bbox(metadata_obj, this_frame_n)
	return ((composite_bbox["x1"]*screen_dimensions["w"]), (composite_bbox["y1"]*screen_dimensions["h"]))

def sponsorship_term_cxy(metadata_obj, this_frame_n):
	screen_dimensions = get_screen_dimensions(metadata_obj, this_frame_n)
	composite_bbox = get_composite_bbox(metadata_obj, this_frame_n)
	sponsorship_bbox = metadata_obj["nameValuePairs"]["frameMetadata"]["internalJSONObject"]["nameValuePairs"][this_frame_n]["internalJSONObject"]["nameValuePairs"]["inference"]["internalJSONObject"]["nameValuePairs"]["boundingBoxSponsored"]["internalJSONObject"]["nameValuePairs"]
	cx = ((sponsorship_bbox["cx"]*screen_dimensions["w"]) - (composite_bbox["x1"]*screen_dimensions["w"]))
	cy = ((sponsorship_bbox["cy"]*screen_dimensions["h"]) - (composite_bbox["y1"]*screen_dimensions["h"]))
	return (cx, cy)

def translate_box(o_x, o_y, box):
	return (box[0] + o_x, box[1] + o_y, box[2] + o_x, box[3] + o_y)


def filter_and_crop_rects(boundary, rects, crop=True):
	result = []
	for r in rects:
		intersection = intersect(boundary, r)
		if intersection:
			if (crop):
				result.append(intersection)
			else:
				result.append(r)
		else:
			result.append(None)
	return result

def frame_similarities(metadata_a, metadata_b, frame_n_a, frame_n_b):
	a_x, a_y = get_composite_location(metadata_a, frame_n_a)
	b_x, b_y = get_composite_location(metadata_b, frame_n_b)
	a_w, a_h = get_composite_dimensions(metadata_a, frame_n_a)
	b_w, b_h = get_composite_dimensions(metadata_b, frame_n_b)
	return exact_overlap_percentage_optimized([(a_x, a_y, a_x+a_w, a_y+a_h)], [(b_x, b_y, b_x+b_w, b_y+b_h)])


def frame_ocr_bbox_similarities(last_frame, current_frame, metadata_a, metadata_b, ocr_a, ocr_b, frame_n_a, frame_n_b, data_donation_failsafe_cache):
	'''
		calculate offset between frames

		translate all boxes in one frame to the other

		find common boundary between frames by getting coordinates of intersection of two frames boundaries after running translation

		crop/delete boxes from both sets that are not within common boundary

		finally run overlap percentage
	'''
	ofse_a = data_donation_failsafe_cache[last_frame["data_donation_uuid"]][str(frame_n_a)]['ocr_frame_sponsored_evaluations']
	ofse_b = data_donation_failsafe_cache[current_frame["data_donation_uuid"]][str(frame_n_b)]['ocr_frame_sponsored_evaluations']

	a_w, a_h = get_composite_dimensions(metadata_a, frame_n_a)
	b_w, b_h = get_composite_dimensions(metadata_b, frame_n_b)

	a_x, a_y = sponsorship_term_cxy(metadata_a, frame_n_a)
	b_x, b_y = sponsorship_term_cxy(metadata_b, frame_n_b)

	offset_x = a_x - b_x
	offset_y = a_y - b_y

	a_bbox = (0, 0, a_w, a_h)
	b_bbox = translate_box(offset_x, offset_y, (0, 0, b_w, b_h))
	common_bbox = intersect(a_bbox, b_bbox)

	
	# Get common coordinate-specific representations for both frames
	boxes_in_a = [ocr_to_rect(x) for x in ocr_a]
	boxes_in_b = [translate_box(offset_x, offset_y, ocr_to_rect(x)) for x in ocr_b] # translate in process

	boxes_in_a = filter_and_crop_rects(common_bbox, boxes_in_a, crop=False)
	boxes_in_b = filter_and_crop_rects(common_bbox, boxes_in_b, crop=False)

	MIN_QUALIFYING_TEXT_CONFIDENCE = 0.5
	levenshtein_similarities = list()
	unintersected_a = [x for x in list(range(len(boxes_in_a))) if (boxes_in_a[x] is not None)]
	unintersected_b = [x for x in list(range(len(boxes_in_b))) if (boxes_in_b[x] is not None)]

	#if ((str(frame_n_a) == "399") and (str(frame_n_b) == "420")):
	#	ipdb.set_trace()

	for a_i in range(len(boxes_in_a)):
		if (boxes_in_a[a_i] is not None) and (not ofse_a[a_i]):
			if (ocr_a[a_i]["confidence"] >= MIN_QUALIFYING_TEXT_CONFIDENCE):
				a = boxes_in_a[a_i]
				for b_i in range(len(boxes_in_b)):
					if (boxes_in_b[b_i] is not None) and (not ofse_b[b_i]):
						if (ocr_b[b_i]["confidence"] >= MIN_QUALIFYING_TEXT_CONFIDENCE):
							b = boxes_in_b[b_i]
							intersection = intersect(a, b)
							#if (ocr_a[a_i]["text"] == "Get your free credit score and lear:" == ocr_b[b_i]["text"]):
							#	ipdb.set_trace()
							if (intersection is not None):
								intersection_pct = (rect_area(intersection) / min(rect_area(a), rect_area(b)))
								if (intersection_pct >= 0.25):
									levenshtein_similarities.append({
											"a_i" : a_i,
											"b_i" : b_i,
											"text_a" : ocr_a[a_i]["text"],
											"text_b" : ocr_b[b_i]["text"],
											"similarity_pct" : sliding_levenshtein_pct(ocr_a[a_i]["text"], ocr_b[b_i]["text"]),
											"intersection_pct" : intersection_pct,
											"ocr_conf_a" : ocr_a[a_i]["confidence"],
											"ocr_conf_b" : ocr_b[b_i]["confidence"]
										})
									unintersected_a = [x for x in unintersected_a if (not (x == a_i))]
									unintersected_b = [x for x in unintersected_b if (not (x == b_i))]
	for a_i in unintersected_a:
		if (ocr_a[a_i]["confidence"] >= MIN_QUALIFYING_TEXT_CONFIDENCE) and (not ofse_a[a_i]):
			levenshtein_similarities.append({
					"a_i" : a_i,
					"b_i" : a_i,
					"text_a" : ocr_a[a_i]["text"],
					"text_b" : ocr_a[a_i]["text"],
					"similarity_pct" : float(),
					"ocr_conf_a" : ocr_a[a_i]["confidence"],
					"ocr_conf_b" : ocr_a[a_i]["confidence"]
				})
	for b_i in unintersected_b:
		if (ocr_b[b_i]["confidence"] >= MIN_QUALIFYING_TEXT_CONFIDENCE) and (not ofse_b[b_i]):
			levenshtein_similarities.append({
					"a_i" : b_i,
					"b_i" : b_i,
					"text_a" : ocr_b[b_i]["text"],
					"text_b" : ocr_b[b_i]["text"],
					"similarity_pct" : float(),
					"ocr_conf_a" : ocr_b[b_i]["confidence"],
					"ocr_conf_b" : ocr_b[b_i]["confidence"]
				})
	# Lastly, remove any text candidates that have doubled up on comparisons
	adjusted_similarities = list()
	for i in range(len(levenshtein_similarities)):
		comparables = list()
		for j in range(len(levenshtein_similarities)):
			if (i != j):
				if ((levenshtein_similarities[i]["a_i"] == levenshtein_similarities[j]["a_i"]) 
					or (levenshtein_similarities[i]["b_i"] == levenshtein_similarities[j]["b_i"])):
					comparables.append(levenshtein_similarities[j])
		if (not any([x["similarity_pct"] > levenshtein_similarities[i]["similarity_pct"] for x in comparables])):
			adjusted_similarities.append(levenshtein_similarities[i])
	return adjusted_similarities
	'''
	# Provided that the common bbox is not None, and that it is at least 75% the area of the smaller bbox
	MIN_BBOXES_IN_FRAME = 1
	MIN_INTERSECTION_OF_BBOXES_PCT = 0.75
	bboxes_are_aligned = ((common_bbox is not None) and ((min(rect_area(a_bbox), rect_area(b_bbox)) / rect_area(common_bbox)) > MIN_INTERSECTION_OF_BBOXES_PCT))
	bboxes_are_populated = (len(boxes_in_a) >= MIN_BBOXES_IN_FRAME and len(boxes_in_b) >= MIN_BBOXES_IN_FRAME)
	if ((bboxes_are_aligned) and (bboxes_are_populated)):
		boxes_in_a = filter_and_crop_rects(common_bbox, boxes_in_a)
		boxes_in_b = filter_and_crop_rects(common_bbox, boxes_in_b)
		return exact_overlap_percentage_optimized(boxes_in_a, boxes_in_b)
	else:
		return float()
	'''

'''
	This function takes two sets of ocrs, and determines whether for the overlapping parts,
	if there is a strong overlap in terms of levenshtein values

	unlike bbox similarities, its not interested in how 
'''





if (__name__ == "__main__"):
	'''
	setA = [(10, 10, 50, 50), (30, 30, 70, 70), (100, 100, 200, 200)]
	setB = [(40, 40, 80, 80), (150, 150, 250, 250)]
	overlap_pct = exact_overlap_percentage_optimized(setA, setB)
	print(f"Optimized exact overlap %: {overlap_pct:.2f}%")
	'''
	pass



























