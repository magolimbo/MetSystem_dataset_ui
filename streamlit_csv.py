import streamlit as st
import json
import datetime
from collections import defaultdict
from typing import Dict, List, Optional
import pandas as pd
from google.cloud import storage
import html as _html
import math
from io import BytesIO

# ──────────────────────────────────────────────────────────────────────────────
# Page setup (must be the first Streamlit call)
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="MetDataset Preview", layout="wide")

# Expand content width + enable overflow so hover zoom can exceed column bounds
st.markdown("""
<style>
.main .block-container { max-width: 100% !important; padding-left: 1rem; padding-right: 1rem; }
div[data-testid="stDataFrame"] { width: 100% !important; }

/* allow zoomed thumbnails / menus to overflow columns */
div[data-testid="column"] { overflow: visible !important; }

/* thumbnail & zoom */
.met-thumb { position: relative; display: inline-block; border-radius: 8px; }
.met-thumb img {
  display: block; width: 100%;
  border-radius: 8px;
  transition: transform .12s ease, box-shadow .12s ease;
  will-change: transform;
  transform-origin: center center;
}
.met-thumb:hover { z-index: 9999; }
.met-thumb:hover img {
  transform: scale(2.2);                  /* zoom factor */
  box-shadow: 0 14px 48px rgba(0,0,0,.35);
  cursor: zoom-in;
}

/* hover label menu (appears to the right of the image) */
.met-thumb .label-menu {
  position: absolute;
  top: 0; left: 100%;
  margin-left: 10px;
  background: rgba(255,255,255,0.98);
  color: #111;
  border: 1px solid rgba(0,0,0,.08);
  box-shadow: 0 10px 30px rgba(0,0,0,.25);
  border-radius: 10px;
  padding: 10px 12px;
  min-width: 200px;
  max-width: 320px;
  font-size: 12px;
  line-height: 1.35;
  opacity: 0;
  transform: translateY(6px);
  transition: opacity .12s ease, transform .12s ease;
  pointer-events: none;              /* purely informational popover */
  white-space: normal;
}
.met-thumb:hover .label-menu {
  opacity: 1;
  transform: translateY(0);
}

/* menu content styling */
.label-menu .menu-title {
  font-weight: 700; margin-bottom: 6px; opacity: .85;
}
.label-menu .label-item { display: flex; gap: 6px; margin: 2px 0; }
.label-menu .label-item .set { font-weight: 600; white-space: nowrap; }
.label-menu .label-item .vals { flex: 1; word-break: break-word; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
# CSV will be provided via the UI (file uploader)
signed_expiry_hours = 12

# Columns that should NOT be treated as annotation sets
NON_ANNOTATION_COLS = {"image_path"}  # add more here if needed

# ──────────────────────────────────────────────────────────────────────────────
# GCS helpers (signed URLs)
# ──────────────────────────────────────────────────────────────────────────────
_client: Optional[storage.Client] = None

def _get_gcs_client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def _parse_gs_uri(gs_uri: str):
    rest = gs_uri[len("gs://"):]
    bucket, _, path = rest.partition("/")
    return bucket, path

def sign_gs_uri(gs_uri: str, hours: int = 12) -> str:
    bucket, path = _parse_gs_uri(gs_uri)
    blob = _get_gcs_client().bucket(bucket).blob(path)
    return blob.generate_signed_url(
        version="v4", expiration=datetime.timedelta(hours=hours), method="GET"
    )

# ──────────────────────────────────────────────────────────────────────────────
# Query params (only for view-mode persistence)
# ──────────────────────────────────────────────────────────────────────────────
def qp_get() -> dict:
    try:
        return dict(st.query_params)
    except Exception:
        return st.experimental_get_query_params()

def qp_set(params: dict):
    try:
        st.query_params.clear()
        try:
            st.query_params.update(params)
        except Exception:
            st.experimental_set_query_params(**params)
    except Exception:
        st.experimental_set_query_params(**params)

# ──────────────────────────────────────────────────────────────────────────────
# Load dataset (CSV)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_records_from_csv(path_or_buffer, expiry_hours: int):
    df = pd.read_csv(path_or_buffer)

    # Ensure required column exists
    if "image_path" not in df.columns:
        raise ValueError("CSV must include an 'image_path' column with paths or URLs to images.")

    # Determine which columns are annotation sets
    annotation_cols = [c for c in df.columns if c not in NON_ANNOTATION_COLS]

    records = []
    for _, row in df.iterrows():
        url = row.get("image_path")
        sign_err = None

        # Sign gs:// URIs
        if isinstance(url, str) and url.startswith("gs://"):
            try:
                url = sign_gs_uri(url, expiry_hours)
            except Exception as e:
                sign_err = str(e)
                url = None

        # Build labels_by_set: {set_name: [value_as_string]}
        labels_by_set: Dict[str, List[str]] = {}
        for col in annotation_cols:
            val = row.get(col)
            if pd.isna(val) or (isinstance(val, float) and math.isnan(val)):
                continue
            sval = str(val).strip()
            if sval:
                labels_by_set[col] = [sval]

        records.append({"url": url, "labels_by_set": labels_by_set, "__sign_error__": sign_err})

    # All sets present anywhere in the CSV
    all_sets = sorted({s for r in records for s in r["labels_by_set"].keys()})
    # All labels per set
    labels_per_set = {
        s: sorted({l for r in records for l in r["labels_by_set"].get(s, [])})
        for s in all_sets
    }
    return records, all_sets, labels_per_set

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
if not uploaded_file:
    st.info("Please upload a .csv file to continue.")
    st.stop()

records, all_sets, labels_per_set = load_records_from_csv(BytesIO(uploaded_file.getvalue()), signed_expiry_hours)

# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────
st.title("MetDataset Preview (CSV)")

# --- Add logo at the very top of the left menu (sidebar) ---
st.sidebar.image("Metsystem_White.png", use_container_width=True)

# Persist view mode in query params (table/grid)
_view_param = qp_get().get("view")
if isinstance(_view_param, list):
    _view_param = _view_param[0] if _view_param else None
_initial_index = 1 if (isinstance(_view_param, str) and _view_param.lower() == "grid") else 0
view_mode = st.sidebar.radio("View mode", ["Table", "Grid"], index=_initial_index)
_desired_view = "grid" if view_mode == "Grid" else "table"
if (_view_param or "table") != _desired_view:
    _p = qp_get(); _p["view"] = _desired_view; qp_set(_p)

st.sidebar.header("Filters")
sets_filter = st.sidebar.multiselect(
    "Required annotation set(s):",
    options=all_sets,
    help="Images must contain ALL selected sets.",
)

selected_labels_per_set: Dict[str, List[str]] = {}
for s in sets_filter:
    selected_labels_per_set[s] = st.sidebar.multiselect(
        f"Required values in '{s}' (AND):",
        options=labels_per_set.get(s, []),
        key=f"labels_{s}",
    )

show_url = st.checkbox("Show URL column", value=False)

# ──────────────────────────────────────────────────────────────────────────────
# Filtering (AND)
# ──────────────────────────────────────────────────────────────────────────────
def record_matches_filters(rec) -> bool:
    for s in sets_filter:
        if s not in rec["labels_by_set"]:
            return False
        req_vals = selected_labels_per_set.get(s, [])
        if req_vals:
            have_vals = rec["labels_by_set"].get(s, [])
            if not all(v in have_vals for v in req_vals):
                return False
    return True

filtered = [r for r in records if record_matches_filters(r)]

# ──────────────────────────────────────────────────────────────────────────────
# Grid view (hover zoom with label menu; NO caption preview)
# ──────────────────────────────────────────────────────────────────────────────
if view_mode == "Grid":
    st.markdown(f"**Images shown:** {len(filtered)} / {len(records)}  |  **Annotation sets:** {len(all_sets)}")

    if not filtered:
        st.info("No images match the current filters.")
        st.stop()

    n_cols, thumb_px = 4, 128

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    for row in chunks(filtered, n_cols):
        cols = st.columns(n_cols)
        for col, rec in zip(cols, row):
            with col:
                if rec.get("url"):
                    # Build the hover menu content with ALL sets that have values
                    items_html = []
                    for set_name in all_sets:
                        vals = rec["labels_by_set"].get(set_name, [])
                        if vals:
                            items_html.append(
                                f'<div class="label-item">'
                                f'<span class="set">{_html.escape(set_name)}:</span>'
                                f'<span class="vals">{_html.escape(", ".join(vals))}</span>'
                                f'</div>'
                            )
                    if not items_html:
                        items_html.append('<div class="label-item"><span class="vals">No labels</span></div>')

                    menu_html = (
                        '<div class="label-menu">'
                        '<div class="menu-title">Labels</div>'
                        + "".join(items_html) +
                        '</div>'
                    )

                    st.markdown(
                        f"""
                        <div class="met-thumb">
                          <img src="{rec['url']}" width="{thumb_px}" loading="lazy"/>
                          {menu_html}
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("No preview")

    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Table view
# ──────────────────────────────────────────────────────────────────────────────
def to_row(rec):
    row = {"Preview": rec["url"]}
    if show_url:
        row["URL"] = rec["url"] or ""
    for s in all_sets:
        vals = rec["labels_by_set"].get(s, [])
        row[s] = ", ".join(vals) if vals else ""
    if rec["__sign_error__"]:
        row["Signing error"] = rec["__sign_error__"]
    return row

df = pd.DataFrame([to_row(r) for r in filtered])
col_order = ["Preview"] + (["URL"] if show_url else []) + all_sets + (["Signing error"] if "Signing error" in df.columns else [])
df = df.reindex(columns=col_order)

st.markdown(f"**Images shown:** {len(filtered)} / {len(records)}  |  **Annotation sets:** {len(all_sets)}")

if df.empty:
    st.info("No images match the current filters.")
else:
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Preview": st.column_config.ImageColumn("Preview", width=128),
            "URL": st.column_config.LinkColumn("URL"),
        },
    )
