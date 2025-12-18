from html import escape

def titleize_key(key: str) -> str:
	"""
	Convert a machine-ish key into a nice title:
	- Replace underscores with spaces
	- Compact multiple spaces
	- Title-case words
	"""
	pretty = key.replace("_", " ").replace("-", " ").strip()
	pretty = " ".join(pretty.split())
	return pretty.title()

def render_value(key: str, value) -> str:
	"""
	Render values safely for HTML. Add emojis for the 'valid' key.
	"""
	if key == "valid":
		return "✅ Valid" if bool(value) else "❌ Invalid"
	if value is None:
		return "—"
	# Render booleans, numbers, and strings consistently
	if isinstance(value, (int, float, bool)):
		return escape(str(value))
	return escape(str(value))

def dict_to_html_table(data: dict) -> str:
	"""
	Return a full HTML document containing a styled table representation of the dict.
	"""
	rows = []
	for k, v in data.items():
		th = escape(titleize_key(k))
		td = render_value(k, v)
		rows.append(f"			<tr><th scope=\"row\">{th}</th><td>{td}</td></tr>")

	return f'''
			<table class="table" role="table">
				{"\n".join(rows)}
			</table>
		'''


def html_complete(this_html) -> str:
	return f"""<!doctype html>
				<html lang="en">
				<head>
					<meta charset="utf-8">
					<meta name="viewport" content="width=device-width, initial-scale=1">
					<style>
						:root {{
							--bg: #0f172a;      /* slate-900 */
							--card: #111827;    /* gray-900 */
							--tone: #e5e7eb;    /* gray-200 */
							--muted: #9ca3af;   /* gray-400 */
							--accent: #3b82f6;  /* blue-500 */
							--ring: rgba(59,130,246,0.35);
						}}
						html, body {{
							background: var(--bg);
							color: var(--tone);
							font: 15px/1.5 ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, Apple Color Emoji, Segoe UI Emoji;
							margin: 0;
							padding: 2rem;
						}}
						.container {{
							max-width: 880px;
							margin: 0 auto;
						}}
						.header {{
							font-size: 1.25rem;
							font-weight: 700;
							margin-bottom: 1rem;
						}}
						.table {{
							width: 100%;
							border-collapse: collapse;
							background: var(--card);
							border-radius: 16px;
							overflow: hidden;
							box-shadow: 0 0 0 1px var(--ring), 0 10px 30px rgba(0,0,0,.35);
						}}
						.table th, .table td {{
							text-align: left;
							padding: .9rem 1.1rem;
							vertical-align: top;
						}}
						.table tr + tr td, .table tr + tr th {{
							border-top: 1px solid rgba(255,255,255,.06);
						}}
						.table th {{
							width: 30%;
							color: var(--muted);
							font-weight: 600;
							letter-spacing: .02em;
						}}
						.caption {{
							margin: .25rem 0 1rem;
							color: var(--muted);
							font-size: .9rem;
						}}
						.badge {{
							display: inline-block;
							font-size: .75rem;
							padding: .15rem .5rem;
							border: 1px solid rgba(255,255,255,.15);
							border-radius: 999px;
							color: var(--tone);
						}}
					</style>
				</head>
				<body>
					<div class="container">
						{this_html}
					</div>
				</body>
				</html>"""