from collections import defaultdict
import math

def aggregate_confidences(items, method="noisy_or", prior=0.5, alpha=1.0, beta=1.0, normalize=True):
	"""
	items: iterable of {'value': any_hashable, 'conf': float in [0,1]}
	method: 'noisy_or' | 'log_odds' | 'beta_mean'
	prior: prior probability for 'log_odds' (default 0.5)
	alpha, beta: Beta prior for 'beta_mean'
	normalize: if True, rescale composite scores to sum to 1 across values
	"""
	by_val = defaultdict(list)
	for it in items:
		p = max(0.0, min(1.0, float(it['confidence'])))
		by_val[it['value']].append(p)

	def noisy_or(ps):
		q = 1.0
		for p in ps:
			q *= (1.0 - p)
		return 1.0 - q

	def log_odds(ps, prior):
		def logit(p): return math.log(p/(1.0-p))
		def inv_logit(x): return 1.0/(1.0+math.exp(-x))
		L = logit(max(1e-12, min(1-1e-12, prior)))
		for p in ps:
			p = max(1e-12, min(1-1e-12, p))
			L += logit(p)
		return inv_logit(L)

	def beta_mean(ps, alpha, beta):
		return (alpha + sum(ps)) / (alpha + beta + len(ps))

	scores = {}
	for v, ps in by_val.items():
		if method == "noisy_or":
			s = noisy_or(ps)
		elif method == "log_odds":
			s = log_odds(ps, prior)
		elif method == "beta_mean":
			s = beta_mean(ps, alpha, beta)
		else:
			raise ValueError("unknown method")
		scores[v] = s

	if normalize:
		S = sum(scores.values()) or 1.0
		for v in scores:
			scores[v] /= S

	# return sorted list of (value, score, count)
	return sorted(({
				"value" : v, 
				"score" : scores[v], 
				"n_candidates" : len(by_val[v])
			} for v in scores), key=lambda x: x["score"], reverse=True)

def alpha_percentage(s: str) -> float:
	"""
	Return percentage of characters in `s` that are alphabetical (A–Z or a–z).
	"""
	if not s:
		return 0.0
	alpha_count = sum(1 for ch in s if ch.isalpha())
	return (alpha_count / len(s))