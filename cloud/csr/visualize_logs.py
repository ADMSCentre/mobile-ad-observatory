import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import plotly.io as pio
from plotly.io import to_html
pio.renderers.default = "browser"  
from typing import Dict, List, Union
from datetime import datetime
import pytz
from zoneinfo import ZoneInfo

COMMON_TIMEZONE_INFO = ZoneInfo("Australia/Brisbane")

MAPPINGS_PLOT = {
		"n_analyses" : {
			"color" : [235, 99, 114],
			"title" : "Active Analyses"
		},
		"n_screen_recordings" : {
			"color" : [227, 116, 196],
			"title" : "Screen Recordings"
		},
		"n_dispatches" : {
			"color" : [177, 116, 227],
			"title" : "Dispatches"
		}
	}


MAPPINGS_BOOLEAN_PLOT = {
		"battery_optimization_unrestricted" : {
			"title" : "Unrestricted Battery Usage",
			"mappings" : {
				"true" : "Unrestricted",
				"false" : "Restricted"
			},
			"colors" : {
				"true" : "rgba(52, 235, 131, 1.0)",
				"false" : "rgba(232, 81, 118, 1.0)"
			}
		},
		"background_processing_status" : {
			"title" : "Background Processing Status",
			"mappings" : {
				"data_saver_and_restricted" : "Restricted On Data Saver",
				"data_saver_and_whitelisted" : "Whitelisted On Data Saver",  
				"unrestricted" : "Unrestricted",
				"unknown" : "Unknown"
			},
			"colors" : {
				"data_saver_and_restricted" : "rgba(232, 81, 118, 1.0)",
				"data_saver_and_whitelisted" : "rgba(237, 189, 57, 1.0)",
				"unrestricted" : "rgba(52, 235, 131, 1.0)",
				"unknown" : "rgba(150, 150, 150, 1.0)"
			}
		},
		"accessibility_services_enabled" : {
			"title" : "Accessibility Services",
			"mappings" : {
				"true" : "On",
				"false" : "Off"
			},
			"colors" : {
				"true" : "rgba(52, 235, 131, 1.0)",
				"false" : "rgba(232, 81, 118, 1.0)"
			}

		},
		"orientation" : {
			"title" : "Device Orientation",
			"mappings" : {
				"portrait" : "Portrait",
				"landscape" : "Landscape",
				"unknown" : "Unknown"
			},
			"colors" : {
				"portrait" : "rgba(78, 77, 179, 1.0)",
				"landscape" : "rgba(250, 132, 47, 1.0)",
				"unknown" : "rgba(150, 150, 150, 1.0)"
			}
		},
	}
def plot_boolean_timeseries_plotly(statistics: dict):
	fig = go.Figure()

	plottables = dict()
	for x in MAPPINGS_PLOT:
		timestamps = sorted(statistics[x].keys())
		plottables[x] = {
			"times" : [datetime.fromtimestamp(int(ts), tz=COMMON_TIMEZONE_INFO) for ts in timestamps],
			"values" : [int(statistics[x][ts]) for ts in timestamps]
		}
		color_str = ", ".join([str(y) for y in MAPPINGS_PLOT[x]["color"]])
		fig.add_trace(go.Scatter(
				x=plottables[x]["times"],
				y=plottables[x]["values"],
				mode='lines+markers',
				line_shape='hv',
				name=MAPPINGS_PLOT[x]["title"],
				fill='tozeroy',  # Fill from line to y=0
				fillcolor=f'rgba({color_str}, 0.1)',  # RGBA: blue with 20% opacity
				line=dict(color=f'rgba({color_str}, 1.0)')
			))

	# Layout
	fig.update_layout(
		title='Boolean Time Series with Transitions',
		xaxis_title='Time',
		template='plotly_white',
		height=500
	)
	return fig

def dict_to_html_table(data: dict) -> str:
	html = ['<table border="1" style="margin:auto;">']
	for key, value in data.items():
		html.append(f"<tr><th>{key}</th><td> {value} </td></tr>")
	html.append('</table>')
	return '\n'.join(html)

def evaluate_field(v):
	if (type(v) is str):
		return v
	return json.dumps(v)

def plot_time_bucketed_boolean_timeline(statistics: dict, bucket_seconds: int = 900):
	# Step 4: Plot with Plotly
	enumerables = list(MAPPINGS_BOOLEAN_PLOT.keys())
	fig = go.Figure()
	for z in enumerables:
		data = statistics[z]
		data = {int(k):v for k,v in data.items()}
		# Step 1: Ensure data keys are Unix timestamps (ints)
		sorted_timestamps = sorted(data.keys())
		values = [data[ts] for ts in sorted_timestamps]

		# Step 2: Bucket each timestamp into time windows
		buckets = {}
		for ts, val in zip(sorted_timestamps, values):
			if not isinstance(ts, int):
				raise ValueError("Timestamp keys must be Unix timestamps (integers).")
			bucket_key = ts - (ts % bucket_seconds)  # floor to nearest bucket
			# Overwrite with the most recent value in that bucket
			buckets[bucket_key] = (ts, val)

		# Step 3: Extract representative points from each bucket
		final_timestamps = [datetime.fromtimestamp(ts, tz=COMMON_TIMEZONE_INFO) for ts, _ in buckets.values()]
		final_values = [val for _, val in buckets.values()]
		y_values = [enumerables.index(z)+1] * len(final_values)
		labels = [MAPPINGS_BOOLEAN_PLOT[z]["mappings"][evaluate_field(val)] for val in final_values] 
		colors = [MAPPINGS_BOOLEAN_PLOT[z]["colors"][evaluate_field(val)] for val in final_values]


		for i in range(len(final_timestamps) - 1):
			fig.add_trace(go.Scatter(
					x=[final_timestamps[i], final_timestamps[i+1]],
					y=[y_values[0], y_values[0]],
					mode='lines',
					line=dict(color=colors[i], width=2),
					showlegend=False,
					hoverinfo='skip'
				))
		fig.add_trace(go.Scatter(
				x=final_timestamps,
				y=y_values,
				mode='markers',
				marker=dict(color=colors, size=8),
				text=labels,
				textfont=dict(size=14, color=colors),
				hovertemplate='Time: %{x}<br>State: %{text}<extra></extra>',
				showlegend=False
			))

	fig.update_layout(
		xaxis_title='Time',
		yaxis=dict(
			showticklabels=True,
			ticktext=[MAPPINGS_BOOLEAN_PLOT[z]["title"] for z in enumerables],
			tickvals=[x+1 for x in list(range(len(enumerables)))],
			showgrid=False,
			zeroline=False,
			range=[0.8-2, 2.2+2]
		),
		template='plotly_white',
		height=400
	)
	return fig

def combine_plotly_figures(figures, rows=None, cols=1, shared_x=True, shared_y=False, titles=None):
	from plotly.subplots import make_subplots
	import plotly.graph_objects as go

	n = len(figures)
	rows = rows or n

	subplot_titles = titles

	fig = make_subplots(
			rows=rows,
			cols=cols,
			shared_xaxes=shared_x,
			shared_yaxes=shared_y,
			subplot_titles=subplot_titles
		)

	for i, single_fig in enumerate(figures):
		r = (i // cols) + 1
		c = (i % cols) + 1
		subplot_id = f'yaxis{"" if (r == 1 and c == 1) else (i+1)}'

		# Copy each trace
		for j, trace in enumerate(single_fig.data):
			trace_copy = trace
			if trace.showlegend and trace.name:
				trace_copy.name = f"{trace.name} (Fig {i+1})"
			fig.add_trace(trace_copy, row=r, col=c)

		# Attempt to extract and apply custom y-axis ticks
		yaxis = single_fig.layout.yaxis
		yaxis_update = {}

		if hasattr(yaxis, 'tickvals') and yaxis.tickvals is not None:
			yaxis_update[f'{subplot_id}.tickvals'] = yaxis.tickvals
		if hasattr(yaxis, 'ticktext') and yaxis.ticktext is not None:
			yaxis_update[f'{subplot_id}.ticktext'] = yaxis.ticktext

		if yaxis_update:
			fig.update_layout(**yaxis_update)

	fig.update_layout(
		height=400 * rows,
		template='plotly_white',
		legend=dict(
			x=-0.15,
			y=0.45,
			xanchor='left',
			yanchor='top',
			orientation='v'
		)
	)

	return fig

TYPES_RANGES = {
	"ad_dispatch_auto" : {
		"title" : "Auto Ad Dispatch",
		"color" : "rgba(0, 255, 157, 1.0)"
	},
	"ad_dispatch_manual" : {
		"title" : "Manual Ad Dispatch",
		"color" : "rgba(0, 255, 157, 1.0)"
	},
	"recording" : {
		"title" : "Screen Recording",
		"color" : "rgba(242, 20, 0, 1.0)"
	},
	"youtube" : {
		"title" : "Youtube",
		"color" : "rgba(242, 0, 28, 1.0)"
	},
	"instagram" : {
		"title" : "Instagram",
		"color" : "rgba(242, 0, 117, 1.0)"
	},
	"tiktok" : {
		"title" : "TikTok",
		"color" : "rgba(13, 13, 13, 1.0)"
	},
	"facebook" : {
		"title" : "Facebook",
		"color" : "rgba(0, 75, 161, 1.0)"
	},
	"facebook-lite" : {
		"title" : "Facebook Lite",
		"color" : "rgba(0, 199, 217, 1.0)"
	},
	"moat" : {
		"title" : "MOAT",
		"color" : "rgba(242, 198, 0, 1.0)"
	},
}

def plot_timeline_ranges(ranges_by_type: Dict[str, List[Dict[str, Union[int, float]]]], 
										merge_threshold_seconds: int = 30) -> go.Figure:
	enumerables = list(ranges_by_type.keys())
	fig = go.Figure()

	for idx, key in enumerate(enumerables):
		raw_ranges = ranges_by_type[key]
		processed = []

		# Normalize and sort
		for entry in raw_ranges:
			start = None if (not "start" in entry) else int(entry["start"])
			end = None if (not "end" in entry) else int(entry["end"])
			start = end if (start is None) else start
			end = start if (end is None) else end
			processed.append((start, end))
		processed.sort()

		# Merge close ranges
		merged = []
		for start, end in processed:
			if not merged:
				merged.append([start, end])
			else:
				prev_start, prev_end = merged[-1]
				if start - prev_end <= merge_threshold_seconds:
					merged[-1][1] = max(prev_end, end)
				else:
					merged.append([start, end])

		# Plot results
		y_value = idx + 1
		for start, end in merged:
			start_dt = datetime.fromtimestamp(start, tz=COMMON_TIMEZONE_INFO)
			end_dt = datetime.fromtimestamp(end, tz=COMMON_TIMEZONE_INFO)
			if start == end:
				fig.add_trace(go.Scatter(
					x=[start_dt],
					y=[y_value],
					mode='markers',
					marker=dict(size=10, symbol='line-ns-open', color=TYPES_RANGES[key]["color"]),
					showlegend=False,
					hovertemplate=f'{key}<br>%{{x}} (no end)<extra></extra>'
				))
			else:
				fig.add_trace(go.Scatter(
					x=[start_dt, end_dt],
					y=[y_value, y_value],
					mode='lines',
					line=dict(width=12, color=TYPES_RANGES[key]["color"]),
					showlegend=False,
					hovertemplate=f'{key}<br>From: %{{x}}<br>To: %{{x}}<extra></extra>'
				))

	# Layout
	fig.update_layout(
		xaxis_title='Time',
		yaxis=dict(
			tickvals=[i + 1 for i in range(len(enumerables))],
			ticktext=[TYPES_RANGES[k]["title"] for k in enumerables],
			range=[0, len(enumerables) + 1],
			showgrid=False
		),
		margin=dict(l=200, r=40, t=40, b=60),

		template='plotly_white',
		title='Range Timelines by Category'
	)

	return fig

def raw_ds_to_dict(x,ref_x):
	adjusted_intervals = [int(y) for y in list(ref_x[list(ref_x.keys())[0]].keys())]
	output_dict = dict()
	for i in range(len(adjusted_intervals)):
		output_dict[str(adjusted_intervals[i])] = len([z for z in x if (z <= adjusted_intervals[i])])
	return output_dict

RDO_DD_PLOT = {
		"rdos" : {
			"color" : [214, 62, 67],
			"title" : "Ads"
		},
		"data_donations" : {
			"color" : [50, 105, 168],
			"title" : "Data Donations"
		}
	}

def plot_numerical_time_series_with_fill(data: dict) -> go.Figure:
	fig = go.Figure()

	for label, timeseries in data.items():
		# Sort timestamps
		sorted_items = sorted(timeseries.items())
		times = [datetime.fromtimestamp(int(ts), tz=COMMON_TIMEZONE_INFO) for ts, _ in sorted_items]
		values = [val for _, val in sorted_items]
		color_str = ", ".join([str(z) for z in RDO_DD_PLOT[label]["color"]])
		fig.add_trace(go.Scatter(
			x=times,
			y=values,
			mode='lines',
			name=RDO_DD_PLOT[label]['title'],
			showlegend=False,
			line=dict(width=2),
			fill='tozeroy',
			fillcolor=f'rgba({color_str},0.1)',  # Will be overridden per trace for distinction
			hovertemplate=f"{RDO_DD_PLOT[label]['title']}<br>%{{x}}<br>Value: %{{y}}<extra></extra>"
		))

	fig.update_layout(
		title='Time Series with Filled Lines',
		xaxis_title='Time',
		yaxis_title='Value',
		template='plotly_white',
		hovermode='x unified'
	)

	return fig

def unix_to_brisbane(unix_timestamp: int, adjust=True) -> str:
	brisbane_tz = pytz.timezone("Australia/Brisbane")
	v = int(unix_timestamp)
	if (adjust):
		v = int(v/1000)
	dt = datetime.fromtimestamp(v, brisbane_tz)
	return dt.strftime("%Y-%m-%d %H:%M:%S %Z%z")

def generate_comprehensive_report_html(this_observer_uuid, joined_at_table, events, statistics, rdos_ts, data_donations_ts):
	statistics_dd_rdos = {
			"data_donations" : raw_ds_to_dict(data_donations_ts, statistics),
			"rdos" : raw_ds_to_dict(rdos_ts, statistics)
		}
	fig_categoricals = plot_boolean_timeseries_plotly(statistics)
	fig_numericals = plot_time_bucketed_boolean_timeline(statistics)
	fig_ranges = plot_timeline_ranges(events)
	fig_dd_rdos = plot_numerical_time_series_with_fill(statistics_dd_rdos)
	fig_composite = combine_plotly_figures([fig_dd_rdos, fig_ranges, fig_categoricals, fig_numericals],
						titles=["Ad Collection Over Time (Data Donations vs. Ads)", "Device Usage Breakdown", None, None])
	title = f"MOAT Comprehensive Statistical Report (CSR) - Observer UUID: {this_observer_uuid}"
	description = "This report summarizes the mobile device activity of the participant in the context of their involvement within the data donation study. It details the number of data donations submitted, the advertisements identified within those donations, and the social platforms where those ads were observed. Additionally, the report outlines periods during which the app's screen recording and analysis functions were active, alongside basic statistics related to the device’s usage during those intervals."
	plot_html = to_html(fig_composite, include_plotlyjs='cdn', full_html=False)
	return f"""
	<div style="background-color:#f9f9f9; padding:20px; margin:30px; border-radius:8px;">
		<div style="text-align:center; margin-bottom:20px; margin-left: 80px; margin-right: 80px;">
			<h2 style="margin:0; font-size:24px;">{title}</h2>
			<p style="margin-top:8px; color:#555;">{description}</p>
			<p>{joined_at_table}</p>
		</div>
		{plot_html}
	</div>
	"""

if (__name__ == "__main__"):
	pass


