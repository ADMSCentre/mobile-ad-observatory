'''
	A concise vesion of the script used in moat_formalizer
'''
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

