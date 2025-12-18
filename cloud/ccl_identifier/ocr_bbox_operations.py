from statistics import median
from math import log, exp

def stitch_lines_then_blocks(
	ocr_boxes,
	join_lines_with=" ",
	# Confidence roll-up controls
	confidence_weight="area",     # "area" | "chars" | "uniform"
	confidence_method="mean",     # "mean" | "geometric" | "min"
	# Big-gap splitter (prevents far-right UI like "Follow" from merging with the title)
	split_gap_factor=3.5,         # split line if gap > 3.5 × median text height
	split_gap_page_frac=0.25,     # or if gap > 25% of page width
	# Optional tunables (usually fine to leave as defaults)
	line_y_tol_factor=0.55,       # same-line vertical tolerance (× median height)
	word_gap_tol_factor=0.40,     # add a space between spans if gap > this × H
	block_y_gap_factor=1.6,       # max vertical gap between lines in same block (× H)
	block_left_tol_factor=1.2,    # how much left edge may drift within a block (× H)
):
	"""
	Group OCR boxes into lines and blocks, split very wide same-row gaps, and
	produce stitched text plus composite confidences at line/block/page levels.

	Args:
		ocr_boxes: list of dicts with keys:
			- x, y, w, h (pixels)
			- text (string)
			- confidence (0..1 or 0..100, optional)
		join_lines_with: separator used when joining spans within a block and
			when composing the page-level "text".
		confidence_weight:
			- "area": weight confidences by span area (w*h)
			- "chars": weight by character count
			- "uniform": equal weight per span
		confidence_method:
			- "mean": weighted arithmetic mean
			- "geometric": weighted geometric mean (penalizes low scores)
			- "min": strict lower bound (the worst span/line dominates)
		split_gap_factor: split a line if an intra-line horizontal gap exceeds
			this times the median text height (page-adaptive).
		split_gap_page_frac: split a line if an intra-line gap exceeds this
			fraction of the page width (computed from spans' min/max x).
		line_y_tol_factor, word_gap_tol_factor, block_y_gap_factor,
		block_left_tol_factor: additional adaptive thresholds (× median height).

	Returns:
		dict with:
			- "text": page-level stitched text (blocks joined with join_lines_with)
			- "texts": list of per-block stitched texts
			- "blocks": list of blocks, each:
				{
					"box": (x1,y1,x2,y2),
					"text": "...",
					"confidence": float|None,
					"lines": [ { "box":..., "text":..., "confidence":..., "spans":[...] }, ... ]
				}
			- "lines": flat list of line dicts (post-splitting)
			- "confidence": page-level composite confidence
			- "_confidence_settings": echo of chosen weighting/method
	"""
	if not ocr_boxes:
		return {"text": "", "texts": [], "blocks": [], "lines": [], "confidence": None}

	def _norm_conf(c):
		if c is None:
			return None
		try:
			c = float(c)
		except Exception:
			return None
		if c < 0:
			c = 0.0
		# accept 0–1 or 0–100
		if c > 1.0:
			if c <= 100.0:
				c = c / 100.0
			else:
				c = 1.0
		return max(0.0, min(1.0, c))

	def _span_weight(s):
		if confidence_weight == "area":
			return max(1.0, float(s["w"]) * float(s["h"]))
		elif confidence_weight == "chars":
			return float(max(1, len(s.get("text", ""))))
		else:
			return 1.0

	def _weighted_conf(pairs):
		"""
		pairs: list of (conf∈[0,1] or None, weight>0).
		Ignores None confidences.
		"""
		pairs = [(c, max(1e-9, w)) for (c, w) in pairs if c is not None]
		if not pairs:
			return None
		if confidence_method == "min":
			return min(c for c, _ in pairs)
		if confidence_method == "geometric":
			w_sum = sum(w for _, w in pairs)
			return exp(sum(w * log(max(1e-9, c)) for c, w in pairs) / w_sum)
		# default: weighted mean
		w_sum = sum(w for _, w in pairs)
		return sum(c * w for c, w in pairs) / w_sum

	# --- normalize spans ---
	spans = []
	for b in ocr_boxes:
		x1, y1, w, h = float(b["x"]), float(b["y"]), float(b["w"]), float(b["h"])
		spans.append({
			"x1": x1, "y1": y1, "x2": x1 + w, "y2": y1 + h,
			"w": w, "h": h, "xc": x1 + w/2.0, "yc": y1 + h/2.0,
			"text": (b.get("text") or "").strip(),
			"confidence": _norm_conf(b.get("confidence", None)),
		})

	# page-adaptive scales
	H = max(1.0, median(s["h"] for s in spans))
	line_y_tol = line_y_tol_factor * H
	word_gap_tol = word_gap_tol_factor * H
	block_y_gap_tol = block_y_gap_factor * H
	block_left_tol = block_left_tol_factor * H

	# page width estimate for relative gap checks
	page_x1 = min(s["x1"] for s in spans)
	page_x2 = max(s["x2"] for s in spans)
	page_w = max(1.0, page_x2 - page_x1)

	def v_overlap(a, c):
		return max(0.0, min(a["y2"], c["y2"]) - max(a["y1"], c["y1"]))

	# --- 1) Build coarse lines by vertical proximity/overlap (no left-edge grouping yet) ---
	spans.sort(key=lambda s: (s["yc"], s["x1"]))
	lines = []
	for s in spans:
		placed = False
		for ln in lines:
			# same line if centers are close OR there's enough vertical overlap
			if abs(s["yc"] - ln["yc_med"]) <= line_y_tol or \
			   v_overlap(s, ln["bbox_span"]) >= 0.3 * min(s["h"], ln["h_med"]):
				ln["spans"].append(s)
				ys = [t["yc"] for t in ln["spans"]]
				hs = [t["h"] for t in ln["spans"]]
				ln["yc_med"] = median(ys)
				ln["h_med"] = median(hs)
				ln["bbox_span"] = {
					"x1": min(ln["bbox_span"]["x1"], s["x1"]),
					"y1": min(ln["bbox_span"]["y1"], s["y1"]),
					"x2": max(ln["bbox_span"]["x2"], s["x2"]),
					"y2": max(ln["bbox_span"]["y2"], s["y2"]),
				}
				placed = True
				break
		if not placed:
			lines.append({
				"spans": [s],
				"yc_med": s["yc"],
				"h_med": s["h"],
				"bbox_span": {"x1": s["x1"], "y1": s["y1"], "x2": s["x2"], "y2": s["y2"]},
			})

	# --- 2) Sort tokens L→R and SPLIT lines by unusually large horizontal gaps ---
	new_lines = []
	for ln in lines:
		ln["spans"].sort(key=lambda t: (t["x1"], t["y1"]))
		segments = []
		curr = []
		prev = None
		for t in ln["spans"]:
			if prev is None:
				curr = [t]
			else:
				gap = t["x1"] - prev["x2"]
				too_far = (gap > split_gap_factor * max(H, ln["h_med"])) or (gap > split_gap_page_frac * page_w)
				if too_far:
					if curr:
						segments.append(curr)
					curr = [t]
				else:
					curr.append(t)
			prev = t
		if curr:
			segments.append(curr)

		# Convert segments into independent line objects
		for seg in segments:
			parts, prev2 = [], None
			span_pairs = []
			for t in seg:
				if t["text"]:
					if prev2 is None:
						parts.append(t["text"])
					else:
						gap = t["x1"] - prev2["x2"]
						if gap > word_gap_tol or gap > 1:
							parts.append(" ")
						parts.append(t["text"])
				span_pairs.append((t.get("confidence", None), _span_weight(t)))
				prev2 = t

			if seg:
				x1 = min(t["x1"] for t in seg); y1 = min(t["y1"] for t in seg)
				x2 = max(t["x2"] for t in seg); y2 = max(t["y2"] for t in seg)
				line_obj = {
					"spans": seg,
					"text": "".join(parts).strip(),
					"box": (x1, y1, x2, y2),
					"left": x1,
					"yc_med": median(t["yc"] for t in seg),
					"h_med": median(t["h"] for t in seg),
				}
				line_obj["_weight"] = sum(w for _, w in span_pairs)
				line_obj["confidence"] = _weighted_conf(span_pairs)
				new_lines.append(line_obj)

	# Replace with split lines
	lines = new_lines

	# --- 3) Group lines into blocks by vertical adjacency and similar left edge ---
	lines.sort(key=lambda ln: (ln["box"][1], ln["left"]))
	blocks = []
	for ln in lines:
		placed = False
		for blk in blocks:
			last = blk["lines"][-1]
			v_gap = ln["box"][1] - last["box"][3]  # distance from previous bottom to this top
			if v_gap <= block_y_gap_tol and abs(ln["left"] - blk["left_med"]) <= block_left_tol:
				blk["lines"].append(ln)
				blk["lefts"].append(ln["left"])
				blk["left_med"] = median(blk["lefts"])
				blk["_weight_sum"] += ln["_weight"]
				placed = True
				break
		if not placed:
			blocks.append({
				"lines": [ln],
				"lefts": [ln["left"]],
				"left_med": ln["left"],
				"_weight_sum": ln["_weight"],
			})

	# --- 4) Compose block texts + confidences and page aggregates ---
	out_blocks, block_texts, block_pairs = [], [], []
	for blk in blocks:
		bx1 = min(l["box"][0] for l in blk["lines"])
		by1 = min(l["box"][1] for l in blk["lines"])
		bx2 = max(l["box"][2] for l in blk["lines"])
		by2 = max(l["box"][3] for l in blk["lines"])
		blk_text = join_lines_with.join([l["text"] for l in blk["lines"] if l["text"]])
		line_pairs = [(l.get("confidence", None), max(1e-9, l.get("_weight", 1.0))) for l in blk["lines"]]
		blk_conf = _weighted_conf(line_pairs)

		out_blocks.append({
			"box": (bx1, by1, bx2, by2),
			"lines": blk["lines"],
			"text": blk_text,
			"confidence": blk_conf,
		})
		block_texts.append(blk_text)
		block_pairs.append((blk_conf, max(1e-9, blk.get("_weight_sum", 1.0))))

	final_text = join_lines_with.join([t for t in block_texts if t])
	global_conf = _weighted_conf(block_pairs)

	return {
		"text": final_text,
		"texts": block_texts,
		"blocks": out_blocks,
		"lines": lines,
		"confidence": global_conf,
		"_confidence_settings": {"weight": confidence_weight, "method": confidence_method},
	}
